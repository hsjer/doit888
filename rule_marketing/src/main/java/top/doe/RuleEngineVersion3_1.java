//package top.doe;
//
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.state.ListState;
//import org.apache.flink.api.common.state.ListStateDescriptor;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.state.StateTtlConfig;
//import org.apache.flink.api.common.time.Time;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.BroadcastStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
//import org.apache.flink.util.Collector;
//import org.roaringbitmap.longlong.Roaring64Bitmap;
//import top.doe.bean.RuleMeta;
//import top.doe.bean.UserEvent;
//import top.doe.calculator_model.RuleCalculator;
//import top.doe.calculators.RuleMode2Calculator;
//
//import java.nio.ByteBuffer;
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * @Author: 深似海
// * @Site: <a href="www.51doit.com">多易教育</a>
// * @QQ: 657270652
// * @Date: 2024/11/17
// * @Desc: 学大数据，上多易教育
// * <p>
// * 静态画像条件：
// * 职业：宝妈
// * 年龄：20-30之间
// * 近半年平均月消费额 ： >1000
// * 动态统计画像条件：
// * 做过的加购物车行为达到 3次以上  且 平均每次加购的金额 > 200 且 最大加购金额 > 400  且 最小加购金额 > 50
// * 规则触发条件：
// * 当她浏览(X)母婴用品(item_id,p1='v1')时触发推送消息；
// * <p>
// * <p>
// * 静态画像条件：
// * 城市： 天津
// * 年龄：  20-30之间
// * 近半年平均月消费额 ： 1000-1200
// * 月平均活跃天数 :  >  20天
// * 动态统计画像条件：
// * 做过的下单行为达到 4次以上  且 平均每次下单的金额 > 800 且 最大加购金额 > 1200  且 最小加购金额 > 600
// * 规则触发条件：
// * 当她浏览(X)母婴用品(item_id,p1='v1')时触发推送消息；
// **/
//
//@Slf4j
//public class RuleEngineVersion3_1 {
//
//    public static void main(String[] args) throws Exception {
//
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
//
//        env.setParallelism(1);
//
//        // 读用户行为数据
//        KafkaSource<String> source = KafkaSource.<String>builder()
//                .setBootstrapServers("doitedu01:9092,doitedu02:9092,doitedu03:9092")
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .setClientIdPrefix("rule_engine_cli_")
//                .setGroupId("rule_engine_group_")
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setTopics("user_event_log")
//                .build();
//
//        DataStreamSource<String> eventSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "event_source");
//
//        // 解析json数据
//        SingleOutputStreamOperator<UserEvent> eventStream = eventSource.map(json -> JSON.parseObject(json, UserEvent.class));
//
//        // 省略关联维度的阶段
//
//        // keyBy
//        KeyedStream<UserEvent, String> keyedEvent = eventStream.keyBy(UserEvent::getAccount);
//
//
//        // 监听规则元数据表( 增删改 )
//        // 创建mysql cdc source 对象
//        MySqlSource<String> cdcSource = MySqlSource.<String>builder()
//                .username("root")
//                .password("ABC123.abc123")
//                .hostname("doitedu01")
//                .port(3306)
//                .databaseList("dw_50")
//                .tableList("dw_50.rule_metadata")
//                .deserializer(new JsonDebeziumDeserializationSchema())
//                .build();
//
//        DataStreamSource<String> cdcStream = env.fromSource(cdcSource, WatermarkStrategy.noWatermarks(), "mysql-cdc");
//
//        // 解析cdc数据
//        SingleOutputStreamOperator<RuleMeta> ruleMetaStream = cdcStream.map(json -> {
//            JSONObject jsonObject = JSON.parseObject(json);
//            String op = jsonObject.getString("op");
//
//            // 获取cdc数据中的after：包含表数据
//            JSONObject data;
//            if (op.equals("d")) {
//                data = jsonObject.getJSONObject("before");
//            } else {
//                data = jsonObject.getJSONObject("after");
//            }
//
//            // 把数据，封装到RuleMeta对象中
//            RuleMeta ruleMeta = JSON.parseObject(data.toJSONString(), RuleMeta.class);
//            // 从中取出bitmap字节，反序列化成 bitmap对象
//            byte[] ruleCrowdBitmapBytes = ruleMeta.getRule_crowd_bitmap_bytes();
//            Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf();
//            bitmap.deserialize(ByteBuffer.wrap(ruleCrowdBitmapBytes));
//
//            // 再把bitmap对象，和op，放入 ruleMeta中
//            ruleMeta.setRule_crowd_bitmap(bitmap);
//            ruleMeta.setOp(op);
//
//
//            return ruleMeta;
//        });
//
//
//        // 广播元数据流
//        MapStateDescriptor<String, String> bcStateDesc = new MapStateDescriptor<>("bc_state", String.class, String.class);
//        BroadcastStream<RuleMeta> ruleMetaBroadcastStream = ruleMetaStream.broadcast(bcStateDesc);
//
//
//        // 连接事件流  和  元数据广播流
//        SingleOutputStreamOperator<String> messages = keyedEvent.connect(ruleMetaBroadcastStream)
//                .process(new KeyedBroadcastProcessFunction<String, UserEvent, RuleMeta, String>() {
//
//                    // 规则运算机池
//                    final HashMap<Integer, RuleCalculator> calculatorPool = new HashMap<>();
//
//                    // 用于缓存最近2分钟的用户行为明细
//                    ListState<UserEvent> eventsBuffer;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//
//                        // 创建listState的描述器，并开启ttl
//                        ListStateDescriptor<UserEvent> desc = new ListStateDescriptor<>("event_buffer", UserEvent.class);
//                        desc.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(2)).build());
//
//                        // 获取listState
//                        eventsBuffer = getRuntimeContext().getListState(desc);
//
//
//                    }
//
//                    @Override
//                    public void processElement(UserEvent userEvent, KeyedBroadcastProcessFunction<String, UserEvent, RuleMeta, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
//
//
//                        // 将最新接收的用户行为数据，缓存到eventsBuffer中
//                        eventsBuffer.add(userEvent);
//
//                        // 遍历各规则的运算机，处理数据
//                        for (Map.Entry<Integer, RuleCalculator> entry : calculatorPool.entrySet()) {
//                            RuleCalculator calculator = entry.getValue();
//
//                            // 判断该运算机是否是新运算机
//                            if (calculator.getNewStatus()) {
//                                // 如果是新运算机，则把缓存中的数据（包括最新的这一条）逐条交给运算机处理
//                                for (UserEvent event : eventsBuffer.get()) {
//                                    calculator.calculate(event, collector);
//                                }
//                                // 新运算机处理完这些数据后，就不再是新运算机了
//                                calculator.setNewStatus(false);
//                            } else {
//                                // 否则，只把最新的这一条数据交给运算机处理
//                                calculator.calculate(userEvent, collector);
//                            }
//                        }
//                    }
//
//                    @Override
//                    public void processBroadcastElement(RuleMeta ruleMeta, KeyedBroadcastProcessFunction<String, UserEvent, RuleMeta, String>.Context context, Collector<String> collector) throws Exception {
//
//                        /* *
//                         *  规则的动态上线 或 动态更新
//                         */
//                        // 接收的元数据有如下操作类型： r/c 增， d/删除,  u/ 更新： 运算机状态下线
//                        if ((ruleMeta.getOp().equals("r") || ruleMeta.getOp().equals("c") || (ruleMeta.getOp().equals("u")) && ruleMeta.getRule_status() == 1)) {
//
//                            // 创建运算机实例对象
//                            RuleCalculator ruleCalculator = new RuleMode2Calculator();
//                            // 初始化运算机
//                            ruleCalculator.initialize(getRuntimeContext(), ruleMeta);
//                            // 放入运算机池
//                            calculatorPool.put(ruleMeta.getId(), ruleCalculator);
//
//                            log.warn("规则上线,规则id:{},目标人群:{}", ruleMeta.getId(), ruleMeta.getRule_crowd_bitmap().toString());
//                        }
//
//
//                        /* *
//                         *  规则的动态下线
//                         */
//                        if (ruleMeta.getOp().equals("d") || ruleMeta.getRule_status() == 0) {
//                            // 从运算机池移除该规则运算机对象
//                            calculatorPool.remove(ruleMeta.getId());
//
//                            log.warn("规则下线,规则id:{}", ruleMeta.getId());
//                        }
//
//                    }
//                });
//
//
//        messages.print();
//
//
//        env.execute();
//
//
//    }
//}

//package top.doe;
//
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.util.Collector;
//import org.apache.http.HttpHost;
//import org.elasticsearch.action.search.SearchRequest;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.client.RequestOptions;
//import org.elasticsearch.client.RestClient;
//import org.elasticsearch.client.RestHighLevelClient;
//import org.elasticsearch.index.query.*;
//import org.elasticsearch.search.builder.SearchSourceBuilder;
//import top.doe.bean.UserEvent;
//
//import java.util.Objects;
//
///**
// * @Author: 深似海
// * @Site: <a href="www.51doit.com">多易教育</a>
// * @QQ: 657270652
// * @Date: 2024/11/17
// * @Desc: 学大数据，上多易教育
// * <p>
//静态画像条件：
//      职业：宝妈
//      年龄：20-30之间
//      近半年平均月消费额 ： >1000
//动态统计画像条件：
//     做过的加购物车行为达到 3次以上  且 平均每次加购的金额 > 200 且 最大加购金额 > 400  且 最小加购金额 > 50
//规则触发条件：
//     当她浏览(X)母婴用品(item_id,p1='v1')时触发推送消息；
//
//
//静态画像条件：
//     城市： 天津
//     年龄：  20-30之间
//     近半年平均月消费额 ： 1000-1200
//     月平均活跃天数 :  >  20天
//动态统计画像条件：
//     做过的下单行为达到 4次以上  且 平均每次下单的金额 > 800 且 最大加购金额 > 1200  且 最小加购金额 > 600
//规则触发条件：
//当她浏览(X)母婴用品(item_id,p1='v1')时触发推送消息；
//
//
//
// **/
//
//public class RuleEngineVersion1_2 {
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
//        // 规则逻辑
//        SingleOutputStreamOperator<String> ruleMatchResult = eventStream.keyBy(UserEvent::getAccount)
//                .process(new KeyedProcessFunction<String, UserEvent, String>() {
//
//                    ValueState<Double> cartCntState;
//                    ValueState<Double> cartSumState;
//                    ValueState<Double> cartMinState;
//                    ValueState<Double> cartMaxState;
//
//                    RestHighLevelClient client;
//                    SearchRequest request;
//
//                    JSONObject message = new JSONObject();
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//
//                        cartCntState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("CART_CNT", Double.class));
//                        cartSumState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("CART_SUM", Double.class));
//                        cartMinState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("CART_MIN", Double.class));
//                        cartMaxState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("CART_MAX", Double.class));
//
//
//                        // es的请求客户端
//                        client = new RestHighLevelClient(RestClient.builder(new HttpHost("doitedu01", 9200, "http")));
//                        request = new SearchRequest("doit50_profile");
//
//                    }
//
//                    @Override
//                    public void processElement(UserEvent userEvent, KeyedProcessFunction<String, UserEvent, String>.Context context, Collector<String> collector) throws Exception {
//
//                        // 更新 sum
//                        if (userEvent.getEvent_id().equals("add_cart")) {
//                            double amt = Double.parseDouble(userEvent.getProperties().get("amt"));
//
//                            cartCntState.update(cartCntState.value() == null ? 1 : cartCntState.value() + 1);
//                            cartSumState.update(cartSumState.value() == null ? amt : cartSumState.value() + amt);
//
//                            cartMinState.update(cartMinState.value() == null ? amt : Math.min(amt, cartMinState.value()));
//                            cartMaxState.update(cartMinState.value() == null ? amt : Math.max(amt, cartMinState.value()));
//
//                        }
//
//
//                        // 先判断动态画像统计结果是否满足条件
//                        if (userEvent.getEvent_id().equals("X") && userEvent.getProperties().get("p1").equals("v1")
//                                && cartCntState.value() != null
//                                && cartSumState.value() / cartCntState.value() > 200
//                                && cartMinState.value() > 50
//                                && cartMaxState.value() > 400) {
//                            // 再判断静态画像条件是否满足
//                            IdsQueryBuilder id = QueryBuilders.idsQuery().addIds(userEvent.getUser_id() + "");
//                            MatchQueryBuilder city = QueryBuilders.matchQuery("tag_01_03", "北京");
//                            RangeQueryBuilder age = QueryBuilders.rangeQuery("tag_01_04").gte(30).lte(40);
//                            MatchQueryBuilder words = QueryBuilders.matchQuery("tag_03_01", "省电");
//
//                            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//                            boolQueryBuilder.must(id).must(city).must(age).must(words);
//
//
//                            request.source(new SearchSourceBuilder().query(boolQueryBuilder));
//                            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
//
//                            // 如果上面的条件查到了结果，则说明该用户是满足这些静态画像条件的
//                            if (Objects.requireNonNull(response.getHits().getTotalHits()).value > 0) {
//                                message.put("user_id", userEvent.getUser_id());
//                                message.put("rule_id", "rule-02-01");
//                                message.put("fire_time", userEvent.getAction_time());
//
//                                collector.collect(message.toJSONString());
//                            }
//                        }
//                    }
//                });
//
//
//        ruleMatchResult.print();
//
//
//        env.execute();
//
//
//    }
//}

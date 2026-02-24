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
///**
// * @Author: 深似海
// * @Site: <a href="www.51doit.com">多易教育</a>
// * @QQ: 657270652
// * @Date: 2024/11/17
// * @Desc: 学大数据，上多易教育
// * <p>
//静态画像条件：
//    职业：宝妈
//    年龄：20-30之间
//    近半年平均月消费额 ： 1000-1200
//动态统计画像条件：
//     做过A行为3+次
//     做过（B行为，E行为 ，Q行为）序列2+次，
//规则触发条件：
//    当她浏览(X)母婴用品(item_id,p1='v1')时触发推送消息；
//**/
//
//public class RuleEngineVersion1_1 {
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
//
//        DataStreamSource<String> eventSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "event_source");
//
//        // 解析json数据
//        SingleOutputStreamOperator<UserEvent> eventStream = eventSource.map(json -> JSON.parseObject(json, UserEvent.class));
//
//        // 省略关联维度的阶段
//
//
//        // 规则逻辑
//        SingleOutputStreamOperator<String> ruleMatchResult = eventStream.keyBy(UserEvent::getAccount)
//                .process(new KeyedProcessFunction<String, UserEvent, String>() {
//
//                    ValueState<Integer> aCntState;
//                    ValueState<Integer> seqCntState;
//                    ValueState<Integer> seqIdxState;
//
//                    String[] eventSeq;
//
//                    RestHighLevelClient client;
//                    SearchRequest request;
//
//                    JSONObject message = new JSONObject();
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//
//                        aCntState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("A_CNT", Integer.class));
//                        seqCntState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("SEQ_CNT", Integer.class));
//                        seqIdxState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("SEQ_IDX", Integer.class));
//
//                        eventSeq = new String[]{"B", "E", "Q"};
//
//
//                        // es的请求客户端
//                        client = new RestHighLevelClient(RestClient.builder(new HttpHost("doitedu01", 9200, "http")));
//                        request = new SearchRequest("doit50_profile");
//
//
//                    }
//
//                    @Override
//                    public void processElement(UserEvent userEvent, KeyedProcessFunction<String, UserEvent, String>.Context context, Collector<String> collector) throws Exception {
//
//
//                        // 统计规则中需要的动态画像标签
//                        /* *
//                         * 动态画像统计的直接方案 —— 全量计算：
//                         *      把收到的用户行为数据，缓存到一个listState中
//                         *      每当用户发生规则定义的触发事件，则遍历一次listState中数据，进行规则的动态画像标签统计
//                         *
//                         * 优点：逻辑清晰简单、直白
//                         * 缺点：每次触发事件到达时，都需要遍历整个listState，运算效率低
//                         *      缓存用户的行为数据无法界定时间区间，导致状态中的数据量过大且在不断增长，会导致内存消耗过大且checkpoint时长超长
//                         *      总之：性能低下，效率低下
//                         */
//
//
//
//
//                        /* *
//                         * 动态画像统计的优化方案 —— 增量滚动计算：
//                         *      来一个用户行为，就去计算更新一次规则所需要的动态画像标签
//                         *      当用户的触发行为发生时，只要判断当前的标签计算结果是否满足规则要求即可；
//                         *      也没有缓存大量的用户行为在状态中，而只是缓存了几个标签计算更新所需要的累计值即可；
//                         */
//
//
//                        // 做过A行为 3+次
//                        if (userEvent.getEvent_id().equals("A")) {
//                            aCntState.update(aCntState.value() == null ? 1 : aCntState.value() + 1);
//                        }
//
//                        //做过（B行为，E行为 ，Q行为）序列2+次
//                        if (userEvent.getEvent_id().equals(eventSeq[seqIdxState.value() == null ? 0 : seqIdxState.value()])) {
//                            // 如果当前接收的行为，正好是行为序列中等待的行为,则
//
//                            // 把等待索引++
//                            int idx = seqIdxState.value() == null ? 1 : seqIdxState.value() + 1;
//
//                            // 判断索引是否已经到达3了,如果已到达，则索引要归0，并把序列的完成次数++
//                            if (idx == 3) {
//                                idx = 0;
//                                seqCntState.update(seqCntState.value() == null ? 1 : seqCntState.value() + 1);
//                            }
//
//                            // 把最新的idx更新到状态中
//                            seqIdxState.update(idx);
//
//                        }
//
//
//                        // 判断当前行为，是否是规则定义的触发事件
//                        if (userEvent.getEvent_id().equals("X") && userEvent.getProperties().get("p1").equals("v1")) {
//
//                            /*
//                             * 静态画像条件：
//                             * 城市 tag_01_03：上海
//                             * 年龄 tag_01_04 ：20-30之间
//                             * 浏览兴趣词 tag_03_01 ：包含 "省电"
//                             *
//                             * 动态统计画像条件：
//                             * 做过A行为3+次
//                             * 做过（B行为，E行为 ，Q行为）序列2+次，
//                             *
//                             * 规则触发条件：
//                             * 当她浏览(X)母婴用品(item_id,p1='v1')时触发推送消息；
//                             */
//
//                            // 先判断动态画像统计结果是否满足条件
//                            if (aCntState.value() != null && aCntState.value() >= 3 && seqCntState.value() != null && seqCntState.value() >= 2) {
//                                // 再判断静态画像条件是否满足
//                                IdsQueryBuilder id = QueryBuilders.idsQuery().addIds(userEvent.getUser_id() + "");
//                                MatchQueryBuilder city = QueryBuilders.matchQuery("tag_01_03", "北京");
//                                RangeQueryBuilder age = QueryBuilders.rangeQuery("tag_01_04").gte(30).lte(40);
//                                MatchQueryBuilder words = QueryBuilders.matchQuery("tag_03_01", "省电");
//
//                                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//                                boolQueryBuilder.must(id).must(city).must(age).must(words);
//
//
//                                request.source(new SearchSourceBuilder().query(boolQueryBuilder));
//                                SearchResponse response = client.search(request, RequestOptions.DEFAULT);
//
//                                // 如果上面的条件查到了结果，则说明该用户是满足这些静态画像条件的
//                                if (response.getHits().getTotalHits().value > 0) {
//                                    message.put("user_id", userEvent.getUser_id());
//                                    message.put("rule_id", "rule-01-01");
//                                    message.put("fire_time", userEvent.getAction_time());
//
//
//                                    collector.collect(message.toJSONString());
//                                }
//
//                            }
//
//                        }
//
//
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

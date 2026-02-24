//package top.doe.calculators;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import com.sun.media.sound.AiffFileReader;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.util.Collector;
//import org.apache.http.HttpHost;
//import org.elasticsearch.action.search.SearchRequest;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.client.RequestOptions;
//import org.elasticsearch.client.RestClient;
//import org.elasticsearch.client.RestHighLevelClient;
//import org.elasticsearch.index.query.*;
//import org.elasticsearch.search.builder.SearchSourceBuilder;
//import org.roaringbitmap.longlong.Roaring64Bitmap;
//import top.doe.bean.RuleMeta;
//import top.doe.bean.UserEvent;
//import top.doe.calculator_model.RuleCalculator;
//import top.doe.utils.EventCompareUtil;
//
//import java.util.Objects;
//
//public class RuleMode1Calculator implements RuleCalculator {
//    ValueState<Double> cntState;
//    ValueState<Double> sumState;
//    ValueState<Double> minState;
//    ValueState<Double> maxState;
//
//    RestHighLevelClient client;
//    SearchRequest request;
//
//    JSONObject message = new JSONObject();
//
//
//    JSONObject realtime_profile_condition;
//    JSONObject static_profile_condition;
//    JSONObject fire_event;
//    int rule_id;
//    Roaring64Bitmap ruleCrowdBitmap;
//
//
//    boolean newStatus;
//
//    @Override
//    public boolean getNewStatus() {
//        return false;
//    }
//
//    @Override
//    public void setNewStatus(boolean newStatus) {
//        this.newStatus = newStatus;
//    }
//
//    @Override
//    public int getRuleId() {
//        return rule_id;
//    }
//
//    @Override
//    public void initialize(RuntimeContext runtimeContext, RuleMeta ruleMeta) throws Exception {
//
//
//        cntState = runtimeContext.getState(new ValueStateDescriptor<Double>("CART_CNT", Double.class));
//        sumState = runtimeContext.getState(new ValueStateDescriptor<Double>("CART_SUM", Double.class));
//        minState = runtimeContext.getState(new ValueStateDescriptor<Double>("CART_MIN", Double.class));
//        maxState = runtimeContext.getState(new ValueStateDescriptor<Double>("CART_MAX", Double.class));
//
//
//        // es的请求客户端
//        client = new RestHighLevelClient(RestClient.builder(new HttpHost("doitedu01", 9200, "http")));
//        request = new SearchRequest("doit50_profile");
//
//        // 获取本规则的静态画像预圈选人群
//        ruleCrowdBitmap = ruleMeta.getRule_crowd_bitmap();
//
//
//        // 规则参数解析
//        JSONObject paramObject = JSON.parseObject(ruleMeta.getRule_param_json());
//        realtime_profile_condition = paramObject.getJSONObject("realtime_profile_condition");
//        static_profile_condition = paramObject.getJSONObject("static_profile_condition");
//        fire_event = paramObject.getJSONObject("fire_event");
//        rule_id = paramObject.getIntValue("rule_id");
//
//    }
//
//    @Override
//    public void calculate(UserEvent userEvent, Collector<String> collector) throws Exception {
//
//        // 判断该用户，是否属于本规则的静态画像目标人群
//        if (!ruleCrowdBitmap.contains(userEvent.getUser_id())) return;
//
//
//        // 更新 sum/cnt/最大值/最小值
//        if (userEvent.getEvent_id().equals(realtime_profile_condition.getString("event_id"))) {
//            double amt = Double.parseDouble(userEvent.getProperties().get(realtime_profile_condition.getString("prop_name")));
//
//            cntState.update(cntState.value() == null ? 1 : cntState.value() + 1);
//            sumState.update(sumState.value() == null ? amt : sumState.value() + amt);
//
//            minState.update(minState.value() == null ? amt : Math.min(amt, minState.value()));
//            maxState.update(maxState.value() == null ? amt : Math.max(amt, maxState.value()));
//
//        }
//
//
//        // 先判断动态画像统计结果是否满足条件
//
//
//        // 判断当前行为，是否是本规则的触发条件行为
//        if (userEvent.getEvent_id().equals(fire_event.getString("event_id"))) {
//
//            JSONArray props = fire_event.getJSONArray("props");
//            for (int i = 0; i < props.size(); i++) {
//                // 取参数：  属性名,  >=  ,属性值
//                JSONObject prop = props.getJSONObject(i);
//                String pName = prop.getString("prop_name");
//                String op = prop.getString("operator");   //    > , between , >=
//                JSONArray pValueArray = prop.getJSONArray("prop_value");  // [10,20]
//
//                // 取当前事件中，参数要求的属性值
//                String curPValueStr = userEvent.getProperties().get(pName);
//
//                boolean propFlag = EventCompareUtil.compareEventProp(op, pValueArray, curPValueStr);
//
//                if (!propFlag) return;
//            }
//
//            // 获取动态画像统计的各条件阈值
//            double avg_prop_value = realtime_profile_condition.getDoubleValue("avg_prop_value");
//            double min_prop_value = realtime_profile_condition.getDoubleValue("min_prop_value");
//            double max_prop_value = realtime_profile_condition.getDoubleValue("max_prop_value");
//
//            // 判断该用户是否已满足所有的动态画像条件
//            if (cntState.value() != null
//                    && sumState.value() / cntState.value() > avg_prop_value
//                    && minState.value() > min_prop_value
//                    && maxState.value() > max_prop_value) {
//
//                message.put("user_id", userEvent.getUser_id());
//                message.put("rule_id", rule_id);
//                message.put("fire_time", userEvent.getAction_time());
//
//                collector.collect(message.toJSONString());
//            }
//
//        }
//
//
//    }
//}
//
//

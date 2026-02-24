//package top.doe.calculators;
//
//import com.alibaba.fastjson.JSONObject;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.api.common.state.KeyedStateStore;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.util.Collector;
//import org.roaringbitmap.longlong.Roaring64Bitmap;
//import top.doe.bean.RuleMeta;
//import top.doe.bean.UserEvent;
//import top.doe.calculator_model.RuleCalculator;
//
//public class RuleModel4Calculator implements RuleCalculator {
//    boolean newStatus;
//    int ruleId;
//
//    String realEventId;
//    int min_event_cnt;
//    String propName;
//    String keyword;
//    int keyword_minCnt;
//    String fireEvent;
//
//    Roaring64Bitmap ruleCrowdBitmap;
//
//    ValueState<Integer> state;
//    ValueState<Integer> state2;
//
//
//    JSONObject message;
//
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
//        return this.ruleId;
//    }
//
//    @Override
//    public void initialize(RuntimeContext runtimeContext, RuleMeta ruleMeta, KeyedStateStore keyedStateStore) throws Exception {
//
//        JSONObject paramObj = JSONObject.parseObject(ruleMeta.getRule_param_json());
//        this.ruleId = paramObj.getIntValue("rule_id");
//        JSONObject realtimeProfileCondition = paramObj.getJSONObject("realtime_profile_condition");
//        realEventId = realtimeProfileCondition.getString("event_id");
//        min_event_cnt = realtimeProfileCondition.getIntValue("min_event_cnt");
//        propName = realtimeProfileCondition.getString("prop_name");
//        keyword = realtimeProfileCondition.getString("keyword");
//        keyword_minCnt = realtimeProfileCondition.getIntValue("min_cnt");
//
//        fireEvent = paramObj.getJSONObject("fire_condition").getString("event_id");
//
//
//        ruleCrowdBitmap = ruleMeta.getRule_crowd_bitmap();
//
//
//        if(runtimeContext!=null) {
//            state = runtimeContext.getState(new ValueStateDescriptor<Integer>(ruleId + "cnt", Integer.class));
//            state2 = runtimeContext.getState(new ValueStateDescriptor<Integer>(ruleId + "cnt2", Integer.class));
//        }else{
//            state = keyedStateStore.getState(new ValueStateDescriptor<Integer>(ruleId + "cnt", Integer.class));
//            state2 = keyedStateStore.getState(new ValueStateDescriptor<Integer>(ruleId + "cnt2", Integer.class));
//
//        }
//
//        message = new JSONObject();
//    }
//
//    @Override
//    public void calculate(UserEvent userEvent, Collector<String> collector) throws Exception {
//
//
//
//        if (!ruleCrowdBitmap.contains(userEvent.getUser_id())) return;
//
//        // 动态画像统计更新
//        if (userEvent.getEvent_id().equals(realEventId)) {
//            state.update(state.value() == null ? 1 : state.value() + 1);
//            if (userEvent.getProperties().get(propName).contains(keyword)) {
//                state2.update(state2.value() == null ? 1 : state2.value() + 1);
//            }
//        }
//
//
//        if (userEvent.getEvent_id().equals(fireEvent)
//                && state != null && state.value() >= min_event_cnt
//                && state2 != null && state2.value() >= keyword_minCnt) {
//            message.put("user_id", userEvent.getUser_id());
//            message.put("fire_time", userEvent.getAccount());
//            message.put("rule_id", ruleId);
//            message.put("event_cnt", state.value());
//            message.put("keyword_cnt", state2.value());
//
//            collector.collect(message.toJSONString());
//        }
//
//
//    }
//}

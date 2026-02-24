package top.doe.calculators;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import redis.clients.jedis.Jedis;
import top.doe.bean.RuleMeta;
import top.doe.bean.UserEvent;
import top.doe.calculator_model.RuleCalculator;
import top.doe.utils.EventCompareUtil;

import java.util.Iterator;

public class RuleModel3Calculator implements RuleCalculator {

    boolean newStatus;

    // 记录序列的完成次数
    ValueState<Integer> seqCntState;

    // 记录序列当前所到达的索引;
    ValueState<Integer> seqIdxState;

    // 事件次数;
    ValueState<Integer> eCntState;

    // 事件属性总和;
    ValueState<Double> eSumState;


    JSONObject cross1;
    JSONArray cross1EventSeq;
    JSONObject cross2;
    JSONObject fireEventCondition;


    int ruleId;

    Jedis jedis;


    JSONObject message = new JSONObject();


    Roaring64Bitmap ruleCrowdBitmap;
    long historyStatisticEndtime;


    @Override
    public boolean getNewStatus() {
        return newStatus;
    }

    @Override
    public void setNewStatus(boolean newStatus) {
        this.newStatus = newStatus;
    }

    @Override
    public int getRuleId() {
        return ruleId;
    }

    @Override
    public void initialize(RuntimeContext runtimeContext, RuleMeta ruleMeta, KeyedStateStore keyedStateStore) throws Exception {

        // 本模型，是需要历史统计数据的
        newStatus = true;

        ruleId = ruleMeta.getId();

        if (runtimeContext != null) {
            // 记录序列的完成次数
            seqCntState = runtimeContext.getState(new ValueStateDescriptor<Integer>(ruleId + "_seq_cnt", Integer.class));

            // 记录序列当前所到达的索引
            seqIdxState = runtimeContext.getState(new ValueStateDescriptor<Integer>(ruleId + "_seq_idx", Integer.class));


            // 事件次数
            eCntState = runtimeContext.getState(new ValueStateDescriptor<Integer>(ruleId + "_e_cnt", Integer.class));


            // 事件属性总和
            eSumState = runtimeContext.getState(new ValueStateDescriptor<Double>(ruleId + "_e_sum", Double.class));
        } else {
            // 记录序列的完成次数
            seqCntState = keyedStateStore.getState(new ValueStateDescriptor<Integer>(ruleId + "_seq_cnt", Integer.class));

            // 记录序列当前所到达的索引
            seqIdxState = keyedStateStore.getState(new ValueStateDescriptor<Integer>(ruleId + "_seq_idx", Integer.class));


            // 事件次数
            eCntState = keyedStateStore.getState(new ValueStateDescriptor<Integer>(ruleId + "_e_cnt", Integer.class));


            // 事件属性总和
            eSumState = keyedStateStore.getState(new ValueStateDescriptor<Double>(ruleId + "_e_sum", Double.class));
        }

        // 预圈选的人群
        ruleCrowdBitmap = ruleMeta.getRule_crowd_bitmap();

        // 跨区间条件的历史统计截止时间
        historyStatisticEndtime = ruleMeta.getHistory_statistic_endtime();

        // 参数解析
        String ruleParamJson = ruleMeta.getRule_param_json();
        JSONObject paramObject = JSON.parseObject(ruleParamJson);

        JSONArray crossRangeRealtimeCondition = paramObject.getJSONArray("cross_range_realtime_condition");
        // 跨区间动态统计条件1参数
        cross1 = crossRangeRealtimeCondition.getJSONObject(0);

        cross1EventSeq = cross1.getJSONArray("event_seq");

        // 跨区间动态统计条件2参数
        cross2 = crossRangeRealtimeCondition.getJSONObject(1);

        // 触发条件参数
        fireEventCondition = paramObject.getJSONObject("fire_event_condition");


        // 创建redis连接客户端
        jedis = new Jedis("doitedu03", 6379);


    }


    @Override
    public void calculate(UserEvent userEvent, Collector<String> collector) throws Exception {

        // 判断该用户是否属于目标人群，如果不属于，直接返回
        // 判断  newStatus = true  且 该数据早于 “历史统计截止时间 ” ，直接返回
        if (!ruleCrowdBitmap.contains(userEvent.getUser_id())
                || userEvent.getAction_time() < historyStatisticEndtime)

            return;

        // 去查一下该用户的历史段的统计结果
        // 查询条件：
        //    cross1：    大 key-> ruleId:条件标识     小 key-> user_id   value: 次数_索引
        //    cross2：    大 key-> ruleId:条件标识     小 key-> user_id   value: 次数_属性和
        if (seqCntState.value() == null) {
            String cross1_cid = cross1.getString("cross_condition_id");
            String cross1_redis_res = jedis.hget(ruleId + ":" + cross1_cid, String.valueOf(userEvent.getUser_id()));

            int seqCnt = 0;
            int seqIdx = 0;
            if (cross1_redis_res != null) {
                String[] split = cross1_redis_res.split("_");
                seqCnt = Integer.parseInt(split[0]);
                seqIdx = Integer.parseInt(split[1]);
            }

            // 将redis中查询到的值，更新到state中
            seqCntState.update(seqCnt);
            seqIdxState.update(seqIdx);


            String cross2_cid = cross2.getString("cross_condition_id");
            String cross2_redis_res = jedis.hget(ruleId + ":" + cross2_cid, String.valueOf(userEvent.getUser_id()));

            int eCount = 0;
            double eSum = 0;
            if (cross2_redis_res != null) {
                String[] split = cross2_redis_res.split("_");
                eCount = Integer.parseInt(split[0]);
                eSum = Integer.parseInt(split[1]);
            }

            // 将redis中查询到的值，更新到state中
            eCntState.update(eCount);
            eSumState.update(eSum);

        }


        /* *
         * 行为序列条件cross1的画像统计
         */
        // 判断该事件是否 = 序列[当前索引] ,如果是: 索引++, 判断索引=序列长度,  序列次数++ ,而且索引要归 0
        int idxInt = seqIdxState.value();
        JSONObject seqEventObj = cross1EventSeq.getJSONObject(idxInt);

        if (userEvent.getEvent_id().equals(seqEventObj.getString("event_id"))
                && userEvent.getAction_time() < (cross1.getLong("end_time") == -1 ? Long.MAX_VALUE : cross1.getLong("end_time"))) {

            boolean prop_equal = true;

            JSONArray props = seqEventObj.getJSONArray("props");
            for (int i = 0; i < props.size(); i++) {

                JSONObject prop = props.getJSONObject(i);
                String op = prop.getString("prop_oper");
                JSONArray valueArray = new JSONArray();
                valueArray.add(prop.getString("prop_value"));

                prop_equal = EventCompareUtil.compareEventProp(op, valueArray, userEvent.getProperties().get(prop.getString("prop_name")).toString());

                if (!prop_equal) break;
            }

            // 如果代码走到这里，说明当前接收的事件，正好是行为序列的动态统计中等待的事件
            if (prop_equal) {
                int idx = seqIdxState.value();
                idx++;
                if (idx == cross1EventSeq.size()) {
                    idx = 0;
                    // 更新完成次数
                    seqCntState.update(seqCntState.value() + 1);
                }
                // 更新索引值
                seqIdxState.update(idx);
            }

        }


        /* *
         * 事件属性平均值条件cross2的画像统计
         */
        // 判断该事件是否=条件中要求的事件，如果是：次数++， 属性sum+=
        if (userEvent.getEvent_id().equals(cross2.getString("event_id"))) {

            // 事件次数+1
            eCntState.update(eCntState.value() + 1);

            // 属性和累加
            eSumState.update(eSumState.value() + Double.parseDouble(userEvent.getProperties().get(cross2.getString("prop_name"))));

        }


        /* *
         * 触发判断
         */
        // 判断该事件是否=条件中的触发事件，如果是：则判断所有动态画像条件是否都满足，如是则发消息

        String pname = fireEventCondition.getString("prop_name");
        String pop = fireEventCondition.getString("prop_oper");
        int pvalue = fireEventCondition.getInteger("prop_value");
        JSONArray arr = new JSONArray();
        arr.add(pvalue);

        if (userEvent.getEvent_id().equals(fireEventCondition.getString("event_id"))
                && EventCompareUtil.compareEventProp(pop, arr, userEvent.getProperties().get(pname))
        ) {

            // 判断两个动态统计条件是否都满足
            int seqCntValue = cross1.getIntValue("seq_cnt_value");
            int eCntValue = cross2.getIntValue("event_cnt_value");
            double eAvgValue = cross2.getDouble("prop_avg_value");

            double avg = eSumState.value() / eCntState.value();
            if (seqCntState.value() >= seqCntValue && eCntState.value() >= eCntValue && avg > eAvgValue) {

                //
                message.put("user_id", userEvent.getUser_id());
                message.put("fire_time", userEvent.getAction_time());
                message.put("rule_id", ruleId);
                message.put("seqCntValue", seqCntState.value());
                message.put("eCntValue", eCntState.value());
                message.put("eAvgValue", avg);

                collector.collect(message.toJSONString());
            }
        }


    }

    @Override
    public void destroy(AbstractStreamOperator<?> operator) {
        // 关闭各种外部链接
        jedis.disconnect();


        // 记录当前的key
        Object preKey = operator.getCurrentKey();

        // 清理状态
        Iterator<Long> iterator = ruleCrowdBitmap.iterator();
        while (iterator.hasNext()) {
            Long user_id = iterator.next();

            // 对每个key进行状态清理
            operator.setCurrentKey(user_id);
            seqCntState.clear();
            seqIdxState.clear();
            eCntState.clear();
            eSumState.clear();
        }

        // 恢复此前的key
        operator.setCurrentKey(preKey);
    }
}

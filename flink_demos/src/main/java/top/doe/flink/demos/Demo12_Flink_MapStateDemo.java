package top.doe.flink.demos;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/10/11
 * @Desc: 学大数据，上多易教育
 * <p>
 * 从socket读如下订单数据
 * {"uid":1,"oid":"o_1","pid":1,"amt":78.8}
 * {"uid":1,"oid":"o_2","pid":2,"amt":68.8}
 * {"uid":1,"oid":"o_3","pid":1,"amt":160.0}
 * {"uid":1,"oid":"o_4","pid":3,"amt":200.0}
 * {"uid":2,"oid":"o_4","pid":2,"amt":120.8}
 * {"uid":2,"oid":"o_5","pid":2,"amt":780.8}
 * {"uid":4,"oid":"o_10","pid":2,"amt":78.8}
 * {"uid":2,"oid":"o_6","pid":3,"amt":68.8}
 * {"uid":3,"oid":"o_7","pid":3,"amt":78.8}
 * {"uid":4,"oid":"o_9","pid":2,"amt":78.8}
 * {"uid":3,"oid":"o_8","pid":2,"amt":78.8}
 * 实时统计：
 * 每个用户，购买的各商品总金额
 **/
public class Demo12_Flink_MapStateDemo {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");


        // 读socket数据
        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);

        // 解析json
        SingleOutputStreamOperator<OrderBean> beanStream = stream.map(json -> JSON.parseObject(json, OrderBean.class));

        // keyBy
        KeyedStream<OrderBean, Integer> keyedStream = beanStream.keyBy(OrderBean::getUid);

        // 运算 (function应该去申请一个状态容器，而且应该是MapState，用来记一个用户的，各个商品的金额
        SingleOutputStreamOperator<String> resultStream = keyedStream.process(new OrderCalcKeyedProcessFunction());

        resultStream.print();


        env.execute();
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderBean implements Serializable {
        private int uid;
        private String oid;
        private int pid;
        private BigDecimal amt;

    }


    public static class OrderCalcKeyedProcessFunction extends KeyedProcessFunction<Integer, OrderBean, String> {

        MapState<Integer, BigDecimal> mapState;


        @Override
        public void open(Configuration parameters) throws Exception {

            RuntimeContext runtimeContext = getRuntimeContext();
            // 获取一个mapState
            mapState = runtimeContext.getMapState(new MapStateDescriptor<Integer, BigDecimal>("mapState", Integer.class, BigDecimal.class));

        }

        @Override
        public void processElement(OrderBean orderBean, KeyedProcessFunction<Integer, OrderBean, String>.Context ctx, Collector<String> out) throws Exception {

            // 从当前的订单中取出订单的商品id和金额
            int pid = orderBean.getPid();
            BigDecimal amt = orderBean.getAmt();

            // 从状态中取出该商品之前的累计值
            BigDecimal stateAmt = mapState.get(pid);
            // 如果状态中没有该商品之前的累计值，则给它一个初始值：0
            if (stateAmt == null) stateAmt = BigDecimal.ZERO;


            // 然后在原来的累计值基础上 加  当前订单的金额
            BigDecimal added = stateAmt.add(amt);

            // 把加完后的结果，更新到状态中去
            mapState.put(pid, added);

            // 输出该用户的所有商品的总金额
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("uid", ctx.getCurrentKey());
            for (Map.Entry<Integer, BigDecimal> entry : mapState.entries()) {
                jsonObject.put("pid", entry.getKey());
                jsonObject.put("amt", entry.getValue());
                out.collect(jsonObject.toJSONString());
            }


        }
    }


}

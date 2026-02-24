package top.doe.flink.excer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class TechValidate {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // a,1,2,3
        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);

        SingleOutputStreamOperator<Tuple4<String, Integer, Integer, Integer>> data = stream.map(new MapFunction<String, Tuple4<String, Integer, Integer, Integer>>() {
            @Override
            public Tuple4<String, Integer, Integer, Integer> map(String s) throws Exception {
                String[] f = s.split(",");
                return Tuple4.of(f[0], Integer.parseInt(f[1]), Integer.parseInt(f[2]), Integer.parseInt(f[3]));
            }
        });
        KeyedStream<Tuple4<String, Integer, Integer, Integer>, String> keyed = data.keyBy(tp -> tp.f0);


        // 接收运算规则
        // aviator的语法： a+b+c
        //{"rule-id":"rule-01","expression":"a+b+c"}
        DataStreamSource<String> rule = env.socketTextStream("doitedu01", 9998);

        MapStateDescriptor<String, String> desc = new MapStateDescriptor<>("bc", String.class, String.class);
        BroadcastStream<String> broadcast = rule.broadcast(desc);
        //DataStream<String> broadcast = rule.broadcast();


        // 连接两个流
        SingleOutputStreamOperator<String> resultStream = keyed.connect(broadcast)
                .process(new KeyedBroadcastProcessFunction<String, Tuple4<String, Integer, Integer, Integer>, String, String>() {
                    // 处理数据流的方法
                    @Override
                    public void processElement(Tuple4<String, Integer, Integer, Integer> dataTuple, KeyedBroadcastProcessFunction<String, Tuple4<String, Integer, Integer, Integer>, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {

                        Integer n1 = dataTuple.f1;
                        Integer n2 = dataTuple.f2;
                        Integer n3 = dataTuple.f3;

                        // 取到广播状态（它里面有规则）
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(desc);

                        // 遍历所有规则，对当前的数据做运算
                        Iterable<Map.Entry<String, String>> entries = broadcastState.immutableEntries();
                        for (Map.Entry<String, String> entry : entries) {
                            String ruleId = entry.getKey();
                            String expression = entry.getValue();

                            // 用表达式引擎来运行这个表达式
                            // a+b+c
                            // 编译表达式
                            Expression expr = AviatorEvaluator.compile(expression);

                            // 准备表达式中的变量的值
                            HashMap<String, Object> expParams = new HashMap<>();
                            expParams.put("a", n1);
                            expParams.put("b", n2);
                            expParams.put("c", n3);

                            // 执行表达式
                            Object result = expr.execute(expParams);


                            JSONObject resObj = new JSONObject();
                            resObj.put("uid", dataTuple.f0);
                            resObj.put("rule_id", ruleId);
                            resObj.put("express", expression);
                            resObj.put("result", result);

                            out.collect(resObj.toJSONString());

                        }


                    }

                    // 处理广播流
                    // {"rule-id":"rule-01","expression":"a+b+c"}
                    @Override
                    public void processBroadcastElement(String ruleJson, KeyedBroadcastProcessFunction<String, Tuple4<String, Integer, Integer, Integer>, String, String>.Context ctx, Collector<String> out) throws Exception {
                        // 获取到广播状态
                        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(desc);

                        // 把广播流中的规则信息数据解析
                        JSONObject jsonObject = JSON.parseObject(ruleJson);
                        String ruleId = jsonObject.getString("rule-id");
                        String expression = jsonObject.getString("expression");

                        // 把规则信息存入广播状态
                        broadcastState.put(ruleId, expression);

                        log.warn("新注入了一个规则,规则id:{},表达式:{}", ruleId, expression);
                    }
                });


        resultStream.print();

        env.execute();


    }
}

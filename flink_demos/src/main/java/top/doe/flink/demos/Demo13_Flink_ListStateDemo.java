package top.doe.flink.demos;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/10/11
 * @Desc: 学大数据，上多易教育
 * <p>
 * 从socket读数据：
 * a,1
 * a,3
 * a,4
 * a,2
 * b,2
 * b,8
 * c,6
 * c,8
 * <p>
 * 输出：根据字母划分，当前的数据 与 前3条数据 的和
 **/
public class Demo13_Flink_ListStateDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");


        // 读socket数据
        // "a,1"
        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);


        // 解析数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleStream = stream.map(
                // 用lambda实现的MapFunction
                line -> {
                    String[] split = line.split(",");
                    return Tuple2.of(split[0], Integer.parseInt(split[1]));
                },

                // 因为上面的function返回的是一个带泛型的类：Tuple2,由于java的泛型擦除机制，导致flink无法推断返回值的具体类型
                // 所以需要用如下代码，显式传递函数返回值类型信息
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                })
        );


        // keyBy
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tupleStream.keyBy(tp -> tp.f0);

        // 运算
        // 当前的数据 与 前3条数据 的和
        SingleOutputStreamOperator<String> resultStream = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, String>() {

            JSONObject jsonObject = new JSONObject();

            ListState<Integer> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("listState", Integer.class));
            }

            @Override
            public void processElement(Tuple2<String, Integer> tp, KeyedProcessFunction<String, Tuple2<String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {

                Iterable<Integer> stateValues = listState.get();

                // 准备一个临时list，来装状态list中的数据
                ArrayList<Integer> tmpList = new ArrayList<>();

                // 迭代状态list中的数据，一边做累加，一边放入临时list
                int sum = 0;
                int i = 0;
                for (Integer v : stateValues) {
                    sum += v;  // 累加
                    tmpList.add(v);  // 放入临时list

                    i++;
                    if (i == 3) break;
                }


                // 更新临时数据列表
                if (tmpList.size() < 3) {
                    // 添加新元素
                    tmpList.add(tp.f1);
                } else {
                    // 移位
                    for (int j = 0; j < tmpList.size() - 1; j++) {
                        tmpList.set(j, tmpList.get(j + 1));
                    }
                    // 添加新元素
                    tmpList.set(2, tp.f1);
                }
                // 把更新后的临时数据列表，覆盖掉状态中的数据
                listState.update(tmpList);


                // 输出结果
                jsonObject.put("key", ctx.getCurrentKey());
                jsonObject.put("sum", sum + tp.f1);

                out.collect(jsonObject.toJSONString());

            }
        });


        resultStream.print();


        env.execute();


    }
}

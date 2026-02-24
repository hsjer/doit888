package top.doe.flink.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/10/10
 * @Desc: 学大数据，上多易教育
 *   从socket服务器读取数据，做单词统计
 **/
public class Demo1_SocketWordcount {

    public static void main(String[] args) throws Exception {

        // 创建编程入口（环境）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 加载数据源得到流
        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9898);

        // 在流上调算子安排运算逻辑
        // 1. 切单词，生成元组对
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleStream = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {

                // 切单词
                String[] split = line.split(" ");

                // 遍历数组的每一个单词，输出
                for (String word : split) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });


        // 2. 然后对上面的结果分组(相同key的数据会发给相同的下游task)
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tupleStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });


        // 3. 在keyedStream调用聚合算子
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = keyedStream.sum("f1");


        // 4.输出结果到控制台
        resultStream.print();


        // 触发执行
        env.execute("my-wordcount");


    }


}

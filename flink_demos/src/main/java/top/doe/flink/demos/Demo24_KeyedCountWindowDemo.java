package top.doe.flink.demos;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo24_KeyedCountWindowDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");


        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);

        // a,10
        // a,20
        // b,15
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapped = stream.map(s -> {
            String[] split = s.split(",");
            return Tuple2.of(split[0], Integer.parseInt(split[1]));
        }).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapped.keyBy(tp -> tp.f0);

        // 字母相同的数据，每5条加在一起 （keyed滚动窗口）
        //keyedStream.countWindow(5,5);
        keyedStream.countWindow(5)
                .sum("f1")/*.print()*/;



        // 字母相同的数据，每隔2条求最近5条的和
        keyedStream.countWindow(5, 2)
                .sum("f1")
                .print();


        env.execute();

    }
}

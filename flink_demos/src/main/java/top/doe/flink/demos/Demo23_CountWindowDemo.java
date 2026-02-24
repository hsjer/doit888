package top.doe.flink.demos;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class Demo23_CountWindowDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");


        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);

        SingleOutputStreamOperator<Integer> intStream = stream.map(s -> Integer.parseInt(s));


        // 每5条数据的和 （滚动窗口）
        SingleOutputStreamOperator<Integer> resultStream = intStream.countWindowAll(5)
                .process(new ProcessAllWindowFunction<Integer, Integer, GlobalWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Integer, Integer, GlobalWindow>.Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception {
                        int sum = 0;
                        for (Integer element : elements) {
                            sum += element;
                        }

                        out.collect(sum);
                    }
                });

        //resultStream.print();


        // 滑动窗口 : 参数1：窗口长度   参数2：滑动步长
        // 触发条件就是：滑动步长
        SingleOutputStreamOperator<Integer> resStream = intStream.countWindowAll(5, 2)
                .sum(0);
        // 打印并行度
        System.out.println(resStream.getParallelism());

        resStream.print();


        env.execute();

    }
}

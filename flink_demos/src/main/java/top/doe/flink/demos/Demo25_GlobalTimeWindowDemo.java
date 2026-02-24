package top.doe.flink.demos;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Demo25_GlobalTimeWindowDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");


        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);

        // 拼接每10秒的数据  （全局滚动处理时间窗口）
        stream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {

                        StringBuilder sb = new StringBuilder();
                        for (String element : elements) {
                            sb.append(element);
                        }
                        out.collect(sb.toString());
                    }
                })
        /* .print()*/;

        // 每隔5秒，拼接最近10秒的数据
        stream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        StringBuilder sb = new StringBuilder();
                        for (String element : elements) {
                            sb.append(element);
                        }
                        out.collect(sb.toString());
                    }
                })
                .print();


        env.execute();


    }
}

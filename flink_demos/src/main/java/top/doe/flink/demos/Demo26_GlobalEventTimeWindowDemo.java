package top.doe.flink.demos;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Demo26_GlobalEventTimeWindowDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");


        // 10,12000
        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);

        SingleOutputStreamOperator<Tuple2<Integer, Long>> mapped = stream.map(new MapFunction<String, Tuple2<Integer, Long>>() {
            @Override
            public Tuple2<Integer, Long> map(String value) throws Exception {
                String[] split = value.split(",");

                return Tuple2.of(Integer.parseInt(split[0]), Long.parseLong(split[1]));
            }
        });

        // 生成watermark
        SingleOutputStreamOperator<Tuple2<Integer, Long>> watermarked = mapped.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<Integer, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((tp, ts) -> tp.f1)

        );


        // 开窗口计算 : 每5秒数据拼接  (事件时间语义的滚动全局窗口）
        watermarked.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<Tuple2<Integer, Long>, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Tuple2<Integer, Long>, String, TimeWindow>.Context context, Iterable<Tuple2<Integer, Long>> elements, Collector<String> out) throws Exception {

                        StringBuilder sb = new StringBuilder();
                        for (Tuple2<Integer, Long> tp : elements) {
                            sb.append(tp.f0);
                        }


                        out.collect(sb.toString());
                    }
                })
        /*.print().setParallelism(1)*/;


        // 每隔2秒，拼接最近6秒的数据（事件时间语义的滑动全局窗口）
        watermarked.windowAll(SlidingEventTimeWindows.of(Time.seconds(6), Time.seconds(2)))
                .process(new ProcessAllWindowFunction<Tuple2<Integer, Long>, String, TimeWindow>() {

                    @Override
                    public void process(ProcessAllWindowFunction<Tuple2<Integer, Long>, String, TimeWindow>.Context context, Iterable<Tuple2<Integer, Long>> elements, Collector<String> out) throws Exception {
                        StringBuilder sb = new StringBuilder();

                        sb.append("window_start:").append(context.window().getStart()).append("->").append("window_end:").append(context.window().getEnd()).append("\t");

                        for (Tuple2<Integer, Long> tp : elements) {
                            sb.append(tp.f0);
                        }


                        out.collect(sb.toString());
                    }
                })
                /*.print()*/;


        // 如果相邻两条数据 间隔 10s以上，则划分窗口 (会话窗口：只有事件时间语义）
        watermarked.windowAll(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<Tuple2<Integer, Long>, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Tuple2<Integer, Long>, String, TimeWindow>.Context context, Iterable<Tuple2<Integer, Long>> elements, Collector<String> out) throws Exception {
                        StringBuilder sb = new StringBuilder();

                        sb.append("window_start:").append(context.window().getStart()).append("->").append("window_end:").append(context.window().getEnd()).append("\t");

                        for (Tuple2<Integer, Long> tp : elements) {
                            sb.append(tp.f0);
                        }

                        out.collect(sb.toString());
                    }
                })
                .print();


        env.execute();


    }
}

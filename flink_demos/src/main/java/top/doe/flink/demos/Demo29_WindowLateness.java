package top.doe.flink.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.time.Duration;

public class Demo29_WindowLateness {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");


        // a,x,1000
        // a,t,2000
        // a,w,3000
        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);

        SingleOutputStreamOperator<Bean> beanStream = stream.map(s -> {

            String[] split = s.split(",");
            return new Bean(split[0], split[1], Long.parseLong(split[2]));
        });


        // 生成watermark
        SingleOutputStreamOperator<Bean> watermarkStream = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Bean>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Bean>() {
                            @Override
                            public long extractTimestamp(Bean bean, long recordTimestamp) {
                                return bean.timestamp;
                            }
                        })

        );


        KeyedStream<Bean, String> keyedStream = watermarkStream.keyBy(b -> b.key);


        // keyed滚动事件时间窗口
        OutputTag<Bean> sideTag = new OutputTag<>("late-data:", TypeInformation.of(Bean.class));

        SingleOutputStreamOperator<String> resultStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                        .allowedLateness(Time.seconds(2)) // 允许迟到2秒,迟到的数据会导致窗口重新触发并输出新的结果
                        .sideOutputLateData(sideTag)  // 真正迟到的数据，放入一个支流（侧输出流、侧流输出）
                        .process(new ProcessWindowFunction<Bean, String, String, TimeWindow>() {
                            @Override
                            public void process(String key, ProcessWindowFunction<Bean, String, String, TimeWindow>.Context context, Iterable<Bean> elements, Collector<String> out) throws Exception {

                                StringBuilder sb = new StringBuilder();
                                sb.append(context.window().getStart())
                                        .append("->")
                                        .append(context.window().getEnd())
                                        .append("_")
                                        .append("cur_key:")
                                        .append(key)
                                        .append("\t");

                                for (Bean element : elements) {
                                    sb.append(element.ch);
                                }

                                out.collect(sb.toString());
                            }
                        });


        // 窗口运算的结果流中，可以获取指定标签的侧流
        SideOutputDataStream<Bean> sideOutput = resultStream.getSideOutput(sideTag);
        sideOutput.print("late-data:");


        resultStream.print("main-out:");


        env.execute();

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean implements Serializable {
        private String key;
        private String ch;
        private long timestamp;
    }

}

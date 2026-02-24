package top.doe.flink.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;

public class Demo28_KeyedEventTimeWindow {

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
                WatermarkStrategy.<Bean>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Bean>() {
                            @Override
                            public long extractTimestamp(Bean bean, long recordTimestamp) {
                                return bean.timestamp;
                            }
                        })

        );


        KeyedStream<Bean, String> keyedStream = watermarkStream.keyBy(b -> b.key);


        // keyed滚动事件时间窗口
        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
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
                })
        /*.print()*/;


        // keyed滑动事件时间窗口
        keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
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
                })
        /*.print()*/;


        // keyed事件时间会话窗口
        // 间隙，也是跟key绑定的
        keyedStream
                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                //.trigger()  // 指定自定义的窗口触发器
                //.evictor()  // 指定自定义的窗口数据移除器 ：工作在触发窗口之前
                //.allowedLateness()
                //.sideOutputLateData()

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
                })


                //.getSideOutput()
                .print();


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

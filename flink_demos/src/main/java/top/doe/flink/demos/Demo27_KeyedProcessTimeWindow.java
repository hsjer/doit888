package top.doe.flink.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class Demo27_KeyedProcessTimeWindow {

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


        KeyedStream<Bean, String> keyedStream = beanStream.keyBy(b -> b.key);


        // 滚动窗口
        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Bean, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Bean, String, String, TimeWindow>.Context context, Iterable<Bean> elements, Collector<String> out) throws Exception {
                        StringBuilder sb = new StringBuilder();
                        sb.append(context.window().getStart())
                                .append("->")
                                .append(context.window().getEnd())
                                .append("\t");

                        for (Bean element : elements) {
                            sb.append(element.ch);
                        }

                        out.collect(sb.toString());
                    }
                })
        /*.print()*/;


        keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .process(new ProcessWindowFunction<Bean, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Bean, String, String, TimeWindow>.Context context, Iterable<Bean> elements, Collector<String> out) throws Exception {
                        StringBuilder sb = new StringBuilder();
                        sb.append(context.window().getStart())
                                .append("->")
                                .append(context.window().getEnd())
                                .append("\t");

                        for (Bean element : elements) {
                            sb.append(element.ch);
                        }

                        out.collect(sb.toString());
                    }
                })
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

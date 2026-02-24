package top.doe.flink.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: '2024/10/16'
 * @Desc: 学大数据，上多易教育
 * 在source上生成watermark
 **/
public class Demo21_Flink_EventTimeSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");


        // {"province":"a","timestamp":1000,"amt":100}
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("g001")
                .setClientIdPrefix("flink-c-")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setBootstrapServers("doitedu01:9092,doitedu02:9092,doitedu03:9092")
                .setTopics("wm-3")
                .build();


        WatermarkStrategy<String> strategy =
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                return JSON.parseObject(element).getLongValue("timestamp");
                            }
                        })
                        // idle策略：如果抽取器在指定时间内一直抽不到时间戳（就是没数据进来），则往后面发idle信号
                        .withIdleness(Duration.ofMillis(5000));
        DataStreamSource<String> stream = env.fromSource(source, strategy, "ss");


        SingleOutputStreamOperator<Od> odStream = stream.map(json -> JSON.parseObject(json, Od.class));

        odStream.keyBy(od -> od.province)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .reduce(new ReduceFunction<Od>() {
//                    @Override
//                    public Od reduce(Od value1, Od value2) throws Exception {
//                        return null;
//                    }
//                });
                .process(new ProcessWindowFunction<Od, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<Od, String, String, TimeWindow>.Context context, Iterable<Od> elements, Collector<String> out) throws Exception {


                        int sum = 0;
                        for (Od od : elements) {
                            sum += od.amount;
                        }


                        HashMap<String, Object> res = new HashMap<>();
                        res.put("province", key);
                        res.put("amount", sum);
                        res.put("window_start", context.window().getStart());
                        res.put("window_end", context.window().getEnd());

                        out.collect(JSON.toJSONString(res));
                    }
                })
                .print();


        env.execute();

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Od implements Serializable {
        private String province;
        private long timestamp;
        private int amount;
    }


}

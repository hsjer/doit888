package doe;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.doe.bean.UserEvent;

import java.util.Comparator;
import java.util.PriorityQueue;

public class UnOrdered2Ordered {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        env.setParallelism(1);

        // 读用户行为数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu01:9092,doitedu02:9092,doitedu03:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setClientIdPrefix("rule_engine_cli_")
                .setGroupId("rule_engine_group_")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setTopics("unorder-test")
                .build();

        DataStreamSource<String> eventSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "event_source");

        // 解析json数据
        SingleOutputStreamOperator<UserEvent> eventStream = eventSource.map(json -> JSON.parseObject(json, UserEvent.class));

        // 省略关联维度的阶段

        // keyBy
        KeyedStream<UserEvent, String> keyedEvent = eventStream.keyBy(UserEvent::getAccount);


        // 开始排序
        keyedEvent.process(new KeyedProcessFunction<String, UserEvent, UserEvent>() {

            ValueState<PriorityQueue<UserEvent>> queueState;
            ValueState<Long> timerState;

            @Override
            public void open(Configuration parameters) throws Exception {

                queueState = getRuntimeContext().getState(new ValueStateDescriptor<PriorityQueue<UserEvent>>("events", TypeInformation.of(new TypeHint<PriorityQueue<UserEvent>>() {
                })));


                timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer_state", Long.class));


            }

            @Override
            public void processElement(UserEvent userEvent, KeyedProcessFunction<String, UserEvent, UserEvent>.Context context, Collector<UserEvent> collector) throws Exception {

                if(timerState.value() != null) {
                    context.timerService().deleteProcessingTimeTimer(timerState.value());
                    timerState.clear();
                }


                PriorityQueue<UserEvent> queue = queueState.value();
                if (queue == null) {
                    // PriorityQueue默认是：小顶堆
                    queue = new PriorityQueue<>(100, new Comparator<UserEvent>() {
                        @Override
                        public int compare(UserEvent o1, UserEvent o2) {
                            return Long.compare(o1.getAction_time(), o2.getAction_time());
                        }
                    });
                    queueState.update(queue);
                }

                queue.add(userEvent);


                if (queue.size() > 10) {
                    UserEvent e = queue.poll();
                    collector.collect(e);
                }

                long timerTime = System.currentTimeMillis() + 10000;
                context.timerService().registerProcessingTimeTimer(timerTime);
                timerState.update(timerTime);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, UserEvent, UserEvent>.OnTimerContext ctx, Collector<UserEvent> out) throws Exception {

                PriorityQueue<UserEvent> queue = queueState.value();
                if (queue != null) {
                    UserEvent e = queue.poll();
                    if(e!=null) {
                        out.collect(e);
                        long timerTime = timestamp+ 10000;
                        ctx.timerService().registerProcessingTimeTimer(timerTime);
                        timerState.update(timerTime);
                    }
                }


            }
        }).print();


        env.execute();
    }
}

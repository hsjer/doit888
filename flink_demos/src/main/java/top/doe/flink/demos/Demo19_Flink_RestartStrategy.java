package top.doe.flink.demos;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class Demo19_Flink_RestartStrategy {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        // 指定savepoint目录，会让系统在启动时就去加载快照来恢复状态
        //D:\ckpt\b9069fae7238f07458b50be9f22896ad\chk-62
        configuration.setString("execution.savepoint.path","file:///d:/ckpt/b9069fae7238f07458b50be9f22896ad/chk-62");


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // 设置失败重启策略
        //env.setRestartStrategy(RestartStrategies.noRestart());  // 失败后不自动重启

        // 固定延时重启策略： 参数1：最多重启次数   参数2：重启的延迟时长
        // 这是默认的重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

        // 指数惩罚重启策略：参数1：初始惩罚延迟时长， 参数2：最大惩罚延迟时长， 参数3：惩罚延迟时长的倍数  参数4：平稳运行重置重发的时长阈值  参数5：惩罚时长的微调比例
        //env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(Time.seconds(1),Time.seconds(10),2,Time.minutes(10),0.01));

        // 故障率重启策略： 在指定观察时间窗口内的故障率不能超过阈值；如果超过则让job直接失败
        // 参数1:指定时长内的最大故障次数
        // 参数2:上面的“指定时长”
        // 参数3: 重启的延迟时长
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.minutes(10),Time.seconds(1)));

        // 回退到集群配置文件中所配置的重启策略
        //env.setRestartStrategy(RestartStrategies.fallBackRestart());

        // 构建一个kafkaSource对象
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("g001")
                .setClientIdPrefix("flink-c-")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setBootstrapServers("doitedu01:9092,doitedu02:9092,doitedu03:9092")
                .setTopics("err-test")
                .build();

        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "sss");

        KeyedStream<String, String> keyedStream = stream.keyBy(e -> "ok");

        keyedStream.process(new KeyedProcessFunction<String, String, String>() {

            ListState<String> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("lst", String.class));
            }

            @Override
            public void processElement(String value, KeyedProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {

                if (RandomUtils.nextInt(1, 10) % 3 == 0 && value.equals("X"))
                    throw new RuntimeException("人造异常来了.............................");

                listState.add(value);

                StringBuilder sb = new StringBuilder();
                for (String s : listState.get()) {
                    sb.append(s);
                }

                out.collect(sb.toString());
            }
        }).print();

        env.execute();


    }
}

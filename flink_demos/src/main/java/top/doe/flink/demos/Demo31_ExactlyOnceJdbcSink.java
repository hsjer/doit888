package top.doe.flink.demos;

import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import javax.sql.XADataSource;

public class Demo31_ExactlyOnceJdbcSink {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");


        // a,1
        // b,2
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("g001")
                .setClientIdPrefix("flink-c-")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setBootstrapServers("doitedu01:9092,doitedu02:9092,doitedu03:9092")
                .setTopics("test-2")
                .build();

        SingleOutputStreamOperator<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "s").slotSharingGroup("g1");

        SingleOutputStreamOperator<Tuple2<String, String>> mapped = stream.map(s -> {
            String[] split = s.split(",");

            // 人造异常
            if(split[0].equals("err") && RandomUtils.nextInt(1,10)%3 ==0) throw new RuntimeException("------------------");


            return Tuple2.of(split[0], split[1]);
        }).slotSharingGroup("g2").returns(TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));


        // 构造一个 exactlyOnce jdbcSink
        SinkFunction<Tuple2<String, String>> sinkFunction = JdbcSink.<Tuple2<String, String>>exactlyOnceSink(
                "insert into test_2 (`key`,num) values (?, ?) on duplicate key update num = ? ",
                (statement, tp) -> {
                    statement.setString(1, tp.f0);
                    statement.setString(2, tp.f1);
                    statement.setString(3, tp.f1);

                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(0)  // 精确一次的场景下要求重试次数为 0
                        .build(),

                JdbcExactlyOnceOptions.builder()
                        .withTransactionPerConnection(true)  // 每个jdbc连接是否只有一个事务管理
                        .build(),

                () -> {
                    MysqlXADataSource xaDataSource = new MysqlXADataSource();
                    xaDataSource.setUrl("jdbc:mysql://doitedu01:3306/doit50");
                    xaDataSource.setUser("root");
                    xaDataSource.setPassword("ABC123.abc123");
                    return xaDataSource;
                }
        );

        mapped.addSink(sinkFunction);

        env.execute();



    }
}

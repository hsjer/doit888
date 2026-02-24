package top.doe.flink.demos;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class SomeB {


    public String doSomeB(StreamExecutionEnvironment env) throws Exception {
        // 构建一个kafkaSource对象
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("g001")
                .setClientIdPrefix("flink-c-")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setBootstrapServers("doitedu01:9092,doitedu02:9092,doitedu03:9092")
                .setTopics("od")
                .build();


        // 用env使用该source获取流
        DataStreamSource<String> stream1 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "随便");
        stream1.filter(s->Integer.parseInt(s)>10).print();


        // 创建mysql cdc source 对象
        MySqlSource<String> cdcSource = MySqlSource.<String>builder()
                .username("root")
                .password("ABC123.abc123")
                .hostname("doitedu01")
                .port(3306)
                .databaseList("doit50")
                .tableList("doit50.t_person")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> stream2 = env.fromSource(cdcSource, WatermarkStrategy.noWatermarks(), "mysql-cdc");

        Sink sink = null;
        stream2.map(s->s).keyBy(s->s).sinkTo(sink);

        env.execute();


        DataStreamSource<String> s = env.socketTextStream("doitedu02", 8899);
        s.map(String::toUpperCase).print();
        env.execute();

        return "ok";

    }
}

package top.doe.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import top.doe.functions.DataGenSource;

public class SourceStreamUtil {

    public static DataStreamSource<String> getStreamFromKafka(StreamExecutionEnvironment env){
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu01:9092,doitedu02:9092,doitedu03:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setClientIdPrefix("rule_engine_cli_")
                .setGroupId("rule_engine_group_")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setTopics("user-event")
                .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "event_source");
    }


    public static DataStreamSource<String> getMetaFromCdc(StreamExecutionEnvironment env){
        MySqlSource<String> cdcSource = MySqlSource.<String>builder()
                .username("root")
                .password("ABC123.abc123")
                .hostname("doitedu01")
                .port(3306)
                .databaseList("dw_50")
                .tableList("dw_50.rule_metadata")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        return env.fromSource(cdcSource, WatermarkStrategy.noWatermarks(), "mysql-cdc");

    }



    public static DataStreamSource<String> getStreamFromDataGen(StreamExecutionEnvironment env){
        return env.addSource(new DataGenSource());
    }
}

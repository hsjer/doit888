package top.doe.flink.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class Demo31_KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://doitedu01:8020/flink_ckpt");

        // 构建一个kafkaSource对象
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setGroupId("g001")
                .setClientIdPrefix("flink-c-")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setBootstrapServers("doitedu01:9092,doitedu02:9092,doitedu03:9092")
                .setTopics(args[0])
                .build();


        // 用env使用该source获取流
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "随便");

        // {"order_id":1,"order_amt":38.8,"order_type":"团购"}

        // json解析
        SingleOutputStreamOperator<Order> orderStream
                = stream.map(json -> JSON.parseObject(json, Order.class));

        // keyBy
        KeyedStream<Order, String> keyedStream = orderStream.keyBy(od -> od.order_type);

        // 聚合
        SingleOutputStreamOperator<Order> resultStream = keyedStream.sum("order_amt");


        // 构造一个kafkaSink对象
        Properties props = new Properties();
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,"120000");

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("doitedu01:9092,doitedu02:9092,doitedu03:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(args[1])
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(props)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();


        // 输出 sink
        resultStream.map(od->JSON.toJSONString(od)).sinkTo(sink);


        // 触发job
        env.execute("恭喜发财");



    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private int order_id;
        private double order_amt;
        private String order_type;

    }
}

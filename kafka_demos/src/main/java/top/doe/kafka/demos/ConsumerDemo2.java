package top.doe.kafka.demos;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;

public class ConsumerDemo2 {
    public static void main(String[] args) throws IOException, InterruptedException {

        Properties props = new Properties();
        // 加载properties配置文件，并自动解析
        props.load(ConsumerDemo2.class.getClassLoader().getResourceAsStream("consumer.properties"));

        // 构造消费者客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);


        //  手动分配主题分区给消费者
        TopicPartition tp0 = new TopicPartition("stu", 0);
        TopicPartition tp1 = new TopicPartition("stu", 1);
        TopicPartition tp2 = new TopicPartition("stu", 2);
        //  手动分配主题分区给消费者
        consumer.assign(Arrays.asList(tp0, tp1, tp2));
        // 手动指定各分区的消费起始位移
        consumer.seek(tp0, 0);
        consumer.seek(tp1, 0);
        consumer.seek(tp2, 0);


        // 拉取数据
        while (true) {

            // 这里所拉取的数据，可能包含多个主题、多个分区的数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            Iterator<ConsumerRecord<String, String>> iter = records.iterator();

            while (iter.hasNext()) {
                // 迭代到一条数据
                ConsumerRecord<String, String> record = iter.next();
                System.out.println(record);
                Thread.sleep(100);
            }

        }


    }
}

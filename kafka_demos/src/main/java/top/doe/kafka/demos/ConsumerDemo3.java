package top.doe.kafka.demos;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;


/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/10/2
 * @Desc: 学大数据，上多易教育
 *   消费组内自动均衡机制测试
 **/
public class ConsumerDemo3 {
    public static void main(String[] args) throws IOException, InterruptedException {

        Properties props = new Properties();
        // 加载properties配置文件，并自动解析
        props.load(ConsumerDemo3.class.getClassLoader().getResourceAsStream("consumer.properties"));
        props.setProperty("group.id","x007");

        // 构造消费者客户端
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("orders"));

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

package top.doe.kafka.excersize;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class DataSource {

    public static void main(String[] args) throws InterruptedException {

        JSONObject jsonObj = new JSONObject();

        String[] events = {
                "add_cart",
                "product_view",
                "thumb_up",
                "favorite",
                "submit_order",
                "order_pay",
                "item_share"
        };


        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "doitedu01:9092,doitedu02:9092,doitedu03:9092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        props.setProperty("retries", "3");
        props.setProperty("batch.size", "1024");
        props.setProperty("max.request.size", "102400");
        props.put("linger.ms", 1000);
        props.put("buffer.memory", 2048);
        //props.put("partitioner.class",null);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        // {"uid":1,"event_id":"add_cart","action_time":14334632589834,"page_url":"/aa/bbb"}
        while(true){
            jsonObj.put("uid", RandomUtils.nextInt(1,100000));
            jsonObj.put("event_id",events[RandomUtils.nextInt(0,events.length)]);
            jsonObj.put("action_time",System.currentTimeMillis());
            jsonObj.put("page_url","/"+ RandomStringUtils.randomAlphabetic(2)+"/"+RandomStringUtils.randomAlphabetic(3));

            String json = jsonObj.toJSONString();

            // 如果有key，则按key的hash散列
            // 如果没有key，则随机挑选
            // producer内部是有Partitioner：RoundRobinPartitioner/defaultPartitioner
            ProducerRecord<String, String> record = new ProducerRecord<>("doit50", json);

            producer.send(record);
            producer.flush();


            Thread.sleep(RandomUtils.nextInt(50,300));

        }

    }
}

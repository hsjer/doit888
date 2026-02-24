package top.doe.kafka.demos;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo1 {
    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        //指定kafka集群的地址
        //props.setProperty("bootstrap.servers", "doitedu01:9092,doitedu02:9092,doitedu03:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "doitedu01:9092,doitedu02:9092,doitedu03:9092");
        //指定key的序列化方式
        props.setProperty("key.serializer", StringSerializer.class.getName());
        //指定value的序列化方式
        props.setProperty("value.serializer", StringSerializer.class.getName());
        //ack模式，取值有0，1，-1（all），all是最慢但最安全的  服务器应答生产者成功的策略
        props.put("acks", "all");
        //这是kafka发送数据失败的重试次数，这个可能会造成发送数据的乱序问题
        props.setProperty("retries", "3");
        //数据发送批次的大小 单位是字节
        props.setProperty("batch.size", "1024");
        //一次数据发送请求所能发送的最大数据量
        props.setProperty("max.request.size", "102400");
        //消息在缓冲区保留的时间，超过设置的值就会被提交到服务端
        props.put("linger.ms", 1000);
        //整个Producer用到总内存的大小，如果缓冲区满了会提交数据到服务端
        //buffer.memory要大于batch.size，否则会报申请内存不足的错误
        props.put("buffer.memory", 2048);


        // 构造生产者客户端对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        String[] devices = {"mi8","oppo6","mate-x","phone-16","mi10","serial-T"};


        for(int i=0;i<1000;i++) {
            // 业务逻辑,在产生数据
            JSONObject dataObj = new JSONObject();

            String uid = RandomStringUtils.randomNumeric(5);
            dataObj.put("uid", uid);
            dataObj.put("event_id", RandomStringUtils.randomAlphabetic(4).toUpperCase());
            dataObj.put("action_time", System.currentTimeMillis());
            dataObj.put("device_type", devices[RandomUtils.nextInt(0, devices.length)]);

            // 把业务逻辑产生的数据，写入kafka
            // 先把业务数据，封装成kafka的生产者record对象

            int partitionId = Integer.parseInt(uid) % 3;
            // 根据我们自己的需求，来进行数据的分区；如果不指定数据的分区，则producer会随机选择一个分区
            ProducerRecord<String, String> record = new ProducerRecord<>("stu",partitionId, uid, dataObj.toJSONString());

            // 然后发送
            producer.send(record);

            Thread.sleep(100);
        }

        // 收尾，把最后一批攒在缓存中的数据刷出去
        producer.flush();
        producer.close();


    }
}

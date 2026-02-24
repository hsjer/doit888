package top.doe.kafka.excersize;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class DataCalculator {

    public static void main(String[] args) throws SQLException {
        ReentrantLock reentrantLock = new ReentrantLock(true);


        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "doitedu01:9092,doitedu02:9092,doitedu03:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("group.id", "g3");
        props.put("enable.auto.commit", "false");  // 禁用自动提交偏移量
        // 如果没有消费偏移量记录，则自动重设为起始offset：latest, earliest, none
        props.put("auto.offset.reset", "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        TopicPartition tp0 = new TopicPartition("doit50", 0);
        TopicPartition tp1 = new TopicPartition("doit50", 1);

        List<TopicPartition> partitionList = Arrays.asList(tp0, tp1);
        consumer.assign(partitionList);


        //
        // 表名： doit50_offsets
        //       topic,partition,offset
        // 从mysql中获取故障前记录的消费位移
        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/doit50", "root", "ABC123.abc123");
        PreparedStatement pst = conn.prepareStatement("select `partition`,`offset` from doit50_offsets where `topic` = ? ");
        pst.setString(1, "doit50");
        ResultSet resultSet = pst.executeQuery();

        HashMap<Integer, Long> offsetsMap = new HashMap<>();
        while (resultSet.next()) {
            int partition = resultSet.getInt("partition");
            long offset = resultSet.getLong("offset");
            offsetsMap.put(partition, offset);
        }

        // 指定各分区的起始位移
        for (TopicPartition tp : partitionList) {
            Long offset = offsetsMap.get(tp.partition());

            // 如果数据库中有该分区的消费位移，则从该位置开始读
            if (offset != null) {
                consumer.seek(tp, offset);
            }
        }


        ArrayList<Integer> buff = new ArrayList<>();
        // 准备一个hashmap来记录本次拉取的数据所到达的偏移量
        HashMap<Integer, Long> partitionOffsetMap = new HashMap<>();


        // 开启拉数据放缓存的线程
        new Thread(new DataPoller(consumer, buff, partitionOffsetMap, reentrantLock)).start();

        // 开启每5秒做统计的定时任务
//        Timer timer = new Timer(true);
//        timer.scheduleAtFixedRate(new CalculateTask(buff, partitionOffsetMap, lock), 0, 5000);

        //
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(new CalculateTask(buff,partitionOffsetMap,reentrantLock),0,5, TimeUnit.SECONDS);


    }

    /**
     * @Author: 深似海
     * @Site: <a href="www.51doit.com">多易教育</a>
     * @QQ: 657270652
     * @Date: 2024/10/4
     * @Desc: 学大数据，上多易教育
     * 数据拉取线程
     **/
    public static class DataPoller implements Runnable {
        KafkaConsumer<String, String> consumer;
        ArrayList<Integer> buff;
        ReentrantLock reentrantLock;
        HashMap<Integer, Long> partitionOffsetMap;

        public DataPoller(KafkaConsumer<String, String> consumer,
                          ArrayList<Integer> buff,
                          HashMap<Integer, Long> partitionOffsetMap,
                          ReentrantLock reentrantLock) {
            this.consumer = consumer;
            this.buff = buff;
            this.partitionOffsetMap = partitionOffsetMap;
            this.reentrantLock = reentrantLock;
        }


        @Override
        public void run() {
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                reentrantLock.lock();
                for (ConsumerRecord<String, String> record : records) {
                    // {"uid":1,"event_id":"add_cart","action_time":14334632589834,"page_url":"/aa/bbb"}
                    String json = record.value();

                    // json解析提取字段
                    JSONObject obj = JSON.parseObject(json);
                    int uid = obj.getIntValue("uid");
                    String eventId = obj.getString("event_id");
                    long actionTime = obj.getLongValue("action_time");

                    // 统计每5秒的 add_cart 的发生次数和人数
                    if ("add_cart".equals(eventId)) {
                        buff.add(uid);
                    }

                    // 可以通过本条数据得到它的偏移量
                    long offset = record.offset();
                    int partitionId = record.partition();
                    // 更新到记录偏移量的共享hashmap中去
                    this.partitionOffsetMap.put(partitionId, offset);
                }
                reentrantLock.unlock();


            }
        }
    }


    /**
     * @Author: 深似海
     * @Site: <a href="www.51doit.com">多易教育</a>
     * @QQ: 657270652
     * @Date: 2024/10/4
     * @Desc: 学大数据，上多易教育
     * 数据实时运算线程
     **/
    public static class CalculateTask implements Runnable {
        ArrayList<Integer> buff;
        HashMap<Integer, Long> partitionOffsetMap;
        ReentrantLock reentrantLock;
        Connection conn;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        public CalculateTask(ArrayList<Integer> buff,
                             HashMap<Integer, Long> partitionOffsetMap,
                             ReentrantLock reentrantLock) throws SQLException {
            this.buff = buff;
            this.partitionOffsetMap = partitionOffsetMap;
            this.reentrantLock = reentrantLock;


            // 构造mysql的jdbc连接
            conn = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/doit50", "root", "ABC123.abc123");

        }


        @SneakyThrows
        @Override
        public void run() {

            // 处理本次计算的启动时间，获取所属的计算窗口
            long l = System.currentTimeMillis();
            long end = (l / 5000) * 5000;
            long start = end - 5000;

            String windowStart = sdf.format(new Date(start));
            String windowEnd = sdf.format(new Date(end));

            System.out.println(String.format("计算线程:%s , 当前时间窗口为: %s - %s ", Thread.currentThread().getId(),windowStart, windowEnd));


            HashMap<Integer, Long> offsets = new HashMap<>();

            int eventCount;
            int eventUsers;

            reentrantLock.lock();
            System.out.println("计算线程"+Thread.currentThread().getId()+"拿到了锁");
            // 事件的发生次数
            eventCount = buff.size();

            HashSet<Integer> uids = new HashSet<>();
            uids.addAll(buff);

            // 事件的发生人数
            eventUsers = uids.size();

            // 从共享的hashmap中取到当前的偏移量信息
            // 放入线程自有的hashmap中
            for (Map.Entry<Integer, Long> entry : this.partitionOffsetMap.entrySet()) {
                offsets.put(entry.getKey(), entry.getValue());
            }

            // 清空buff
            buff.clear();
            reentrantLock.unlock();
            System.out.println("计算线程"+Thread.currentThread().getId()+"释放了锁");


            try {
                // 关闭自动事务提交
                conn.setAutoCommit(false);

                // 输出结果到mysql
                PreparedStatement pst = conn.prepareStatement("insert into w5s_rpt values(?,?,?,?)");
                // window_start,window_end,event_count,event_users
                pst.setString(1, windowStart);
                pst.setString(2, windowEnd);
                pst.setInt(3, eventCount);
                pst.setInt(4, eventUsers);
                pst.execute();

                // 更新偏移量

                PreparedStatement pst2 = conn.prepareStatement("insert into doit50_offsets values(?,?,?) on duplicate key update offset = ?");
                for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
                    pst2.setString(1, "doit50");
                    pst2.setInt(2, entry.getKey());
                    pst2.setLong(3, entry.getValue());
                    pst2.setLong(4, entry.getValue());
                    pst2.execute();
                }

                // 提交事务
                conn.commit();
            } catch (Exception e) {
                e.printStackTrace();
                // 回滚事务
                conn.rollback();

            }

        }
    }


}

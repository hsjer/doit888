package top.doe.spark_sql;


import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/25
 * @Desc: 学大数据，上多易教育
 *
 * 有如下数据： sql_data/hybrid/y.data
 * 1,[{"event_id":"c","action_time":1727232579000},{"event_id":"a","action_time":1727232579000}]
 *
 * 统计如下需求：
 *    每5分钟，每类事件的发生次数和发生人数
 *    每个用户，发生次数最多的前5类事件及其发生次数
 *
 **/
public class _03_Excersize {
    public static void main(String[] args) {


        SparkSession spark = SparkSession.builder()
                .appName("")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> rdd = sc.textFile("sql_data/hybrid/y.data");
        JavaRDD<EventInfo> beanRdd = (JavaRDD<EventInfo>) rdd.flatMap(new FlatMapFunction<String, EventInfo>() {

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            @Override
            public Iterator<EventInfo> call(String s) throws Exception {

                // 1,[{"event_id":"c","action_time":1727232579000},{"event_id":"a","action_time":1727232579000}]
                // 定义正则表达式
                String regex = "(\\d+),(.*)";

                // 编译正则表达式
                Pattern pattern = Pattern.compile(regex);
                Matcher matcher = pattern.matcher(s);

                // 如果能够匹配正则，则解析数据，返回迭代器
                if (matcher.find()) {
                    // 提取并打印匹配到的部分
                    String part1 = matcher.group(1);
                    String part2 = matcher.group(2);

                    // json解析
                    List<EventInfo> eventInfos = JSON.parseArray(part2, EventInfo.class);

                    for (EventInfo e : eventInfos) {
                        // 填充javabean的uid
                        e.setUid(Integer.parseInt(part1));

                        // 处理时间取整成5分钟
                        long actionTime = e.getAction_time();
                        int i = 5 * 60 * 1000;
                        long floor = (actionTime/i)*i;
                        String format = sdf.format(new Date(floor));

                        e.setMin5(format);

                    }

                    return eventInfos.iterator();
                }

                // 否则，返回空迭代器
                return Collections.emptyIterator();
            }
        });


        // 先把rdd转成dataframe
        Dataset<Row> df = spark.createDataFrame(beanRdd, EventInfo.class);
        // 注册成表
        df.createOrReplaceTempView("events");
        spark.sql("select * from events").show();
        /*
            +-------------+--------+-------------------+---+
            |  action_time|event_id|               min5|uid|
            +-------------+--------+-------------------+---+
            |1727232571000|       a|2024-09-25 10:45:00|  1|
            |1727232572000|       b|2024-09-25 10:45:00|  1|
            |1727232573000|       a|2024-09-25 10:45:00|  1|
            |1727232579000|       b|2024-09-25 10:45:00|  2|
            |1727232579000|       b|2024-09-25 10:45:00|  2|
            |1727232579000|       b|2024-09-25 10:45:00|  2|
            |1727232579000|       a|2024-09-25 10:45:00|  3|
            |1727232579000|       c|2024-09-25 10:45:00|  4|
            |1727232610000|       c|2024-09-25 10:50:00|  4|
            |1727232610000|       d|2024-09-25 10:50:00|  4|
            |1727232579000|       a|2024-09-25 10:45:00|  5|
            |1727232579000|       e|2024-09-25 10:45:00|  5|
            |1727232610000|       a|2024-09-25 10:50:00|  5|
            |1727232579000|       c|2024-09-25 10:45:00|  1|
            |1727232610000|       a|2024-09-25 10:50:00|  1|
            |1727232579000|       c|2024-09-25 10:45:00|  2|
            |1727232579000|       a|2024-09-25 10:45:00|  2|
            |1727232610000|       a|2024-09-25 10:50:00|  2|
            +-------------+--------+-------------------+---+
         */


        // 每5分钟，每类事件的发生次数和发生人数
        spark.sql("SELECT min5,event_id,count(1) as cnt,count(distinct uid) as u   " +
                "from events   " +
                "group by min5,event_id").show();



        // 每个用户，发生次数最多的前5类事件及其发生次数
        Dataset<Row> resDs = spark.sql("WITH TMP AS (\n" +
                "SELECT uid,event_id,count(1) as cnt\n" +
                "       ,row_number() over(partition by uid order by count(1) desc) as rn\n" +
                "from events\n" +
                "group by uid,event_id\n" +
                ")\n" +
                "SELECT uid,event_id,cnt,rn\n" +
                "FROM tmp \n" +
                "WHERE rn<=5");

        // 假如要对上面的统计结果，根据event_id去查询http外部服务，补充数据
        JavaRDD<Row> resRDD = resDs.javaRDD();


        //-----写法1：用map算子来做
        JavaRDD<EventBean> codeRDD = resRDD.map(row -> {
            // 解析row   uid,event_id,cnt,rn
            int uid = row.getAs("uid");
            String event_id = row.getAs("event_id");
            long cnt = row.getAs("cnt");
            int rn = row.getAs("rn");

            // 构造http的请求客户端
            CloseableHttpClient client = HttpClients.createDefault();

            // 构造请求
            HttpGet httpGet = new HttpGet("http://localhost:8080/api/get_event?event_id=" + event_id);

            // 发起请求
            CloseableHttpResponse response = client.execute(httpGet);

            // 解析响应
            String json = EntityUtils.toString(response.getEntity());

            int eventCode = JSON.parseObject(json).getIntValue("event_code");

            // 关闭http客户端
            client.close();

            return new EventBean(uid, event_id, eventCode, cnt, rn);
        });
        //codeRDD.foreach(b-> System.out.println(b));


        //-----写法1：用mapPartitions算子来做
        JavaRDD<EventBean> resultRdd = resRDD.mapPartitions(new FlatMapFunction<Iterator<Row>, EventBean>() {
            @Override
            public Iterator<EventBean> call(Iterator<Row> rowIterator) throws Exception {
                // 构造http的请求客户端
                CloseableHttpClient client = HttpClients.createDefault();

                Stream<Row> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(rowIterator, Spliterator.ORDERED), false);

                Stream<EventBean> resStream = stream.map(row -> {
                    // 解析row   uid,event_id,cnt,rn
                    int uid = row.getAs("uid");
                    String event_id = row.getAs("event_id");
                    long cnt = row.getAs("cnt");
                    int rn = row.getAs("rn");

                    EventBean bean = new EventBean(uid, event_id, null, cnt, rn);


                    // 发起请求
                    HttpGet httpGet = new HttpGet("http://localhost:8080/api/get_event?event_id=" + event_id);
                    try {
                        CloseableHttpResponse response = client.execute(httpGet);
                        String json = EntityUtils.toString(response.getEntity());
                        int eventCode = JSON.parseObject(json).getIntValue("event_code");

                        // 把得到的事件码，填充到要返回的javaBean中
                        bean.setEvent_code(eventCode);

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    return bean;
                });


                return resStream.iterator();
            }
        });
        resultRdd.foreach(b-> System.out.println(b));




    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static  class EventInfo {
        private int uid;
        private String event_id;
        private long action_time;
        private String min5;
        private int event_code;

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static  class EventBean {
        private int uid;
        private String event_id;
        private Integer event_code;
        private long cnt;
        private long rn;


    }

}

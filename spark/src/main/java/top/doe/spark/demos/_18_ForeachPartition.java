package top.doe.spark.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.roaringbitmap.RoaringBitmap;
import scala.Tuple2;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Iterator;
import java.util.List;

public class _18_ForeachPartition {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("xxx");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> strRdd = sc.textFile("spark_data/excersize_3/input/order.data");

        // 收集: 1，主要用于测试；
        // 2，有时候复杂的作业中，需要先用rdd去做一个运算，得到少量结果后收集到driver端
        // 然后这个收集到的数据集合，可以广播给下一个rdd运算作业使用
        // 要注意的点：
        List<String> collect = strRdd.collect();
        System.out.println(collect);

        long count = strRdd.count();
        System.out.println(count);

        // {"uid":1,"oid":"o_1","pid":1,"amt":78.8}
        // {"uid":1,"oid":"o_2","pid":2,"amt":68.8}
        JavaRDD<Order> orderRDD = strRdd.map(s -> JSON.parseObject(s, Order.class));

        JavaPairRDD<Integer, Order> kvRdd = orderRDD.keyBy(od -> od.pid);


        // 统计: 每种商品的 购买单数、总额、人数,写入 mysql
        JavaPairRDD<Integer, Agg> aggRdd = kvRdd.aggregateByKey(
                new Agg(),
                (agg, od) -> {
                    agg.order_count++;
                    agg.amt = agg.amt.add(BigDecimal.valueOf(od.amt));
                    agg.userBitmap.add(od.uid);


                    return agg;
                },
                (agg1, agg2) -> {
                    agg1.order_count += agg2.order_count;
                    agg1.userBitmap.or(agg2.userBitmap);
                    agg1.amt = agg1.amt.add(agg2.amt);
                    return agg1;
                }
        );

        // 利用 foreachPartition来输出
        aggRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, Agg>>>() {
            @Override
            public void call(Iterator<Tuple2<Integer, Agg>> partitionIterator) throws Exception {

                Connection connection = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/doit50", "root", "ABC123.abc123");
                PreparedStatement stmt = connection.prepareStatement("insert into order_tj values (?,?,?,?)");


                while (partitionIterator.hasNext()) {
                    Tuple2<Integer, Agg> kv = partitionIterator.next();
                    Integer pid = kv._1;
                    Agg agg = kv._2;

                    // 将agg中的信息，写入数据表
                    stmt.setInt(1, pid);
                    stmt.setInt(2, agg.order_count);
                    stmt.setInt(3, agg.userBitmap.getCardinality());
                    stmt.setBigDecimal(4, agg.amt);

                    stmt.execute();

                }

                stmt.close();
                connection.close();
            }
        });
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order implements Serializable {
        private int uid;
        private String oid;
        private int pid;
        private double amt;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Agg implements Serializable {
        private int order_count;
        private BigDecimal amt = BigDecimal.ZERO;
        RoaringBitmap userBitmap = RoaringBitmap.bitmapOf();
    }


}

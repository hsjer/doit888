package top.doe.spark.demos;


import com.alibaba.fastjson.JSON;
import lombok.*;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.roaringbitmap.RoaringBitmap;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/11
 * @Desc: 学大数据，上多易教育
 * 输入数据：
 * {"uid":1,"oid":"o_1","pid":1,"amt":78.8}
 * {"uid":1,"oid":"o_2","pid":2,"amt":68.8}
 * {"uid":1,"oid":"o_3","pid":1,"amt":160.0}
 * <p>
 * mysql数据库维表
 * pid,pname,category,brand
 * 1,燃魂咖啡,咖啡饮料,雀巢
 * 2,特仑苏,牛奶,伊利
 * 3,深黑速溶,咖啡饮料,雀巢
 * <p>
 * 需求：
 * 各品类的购买人数、购买总额，人购买金额
 * 各品牌的购买人数、购买总额、人购买金额
 * <p>
 * 各品牌中，购买的订单数最多的前2种商品，及其购买订单数、人数、总金额
 * <p>
 * <p>
 * with tmp as (
 * select
 * brand,
 * pid,
 * count(1) as od_cnt,
 * count(distinct uid) as user_cnt,
 * sum(amt) as amt_sum
 * from od join pd on od.pid = pd.pid
 * group by brand,pid
 * )
 * select  *
 * from (
 * select
 * brand,
 * pid,
 * od_cnt,
 * user_cnt,
 * amt_sum,
 * row_number() over(partition by brand order by od_cnt desc) as rn
 * from tmp
 * )
 * where rn<=2
 **/

public class _07_Xecersize_1_sort {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.set("spark.default.parallelism", "2");
        conf.setAppName("name");


        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("./spark_data/excersize_3/input/order.data", 2);

        // 查询数据
        //  {"uid":1,"oid":"o_1","pid":1,"amt":78.8,"category":"咖啡","brand":"雀巢"}
        JavaRDD<Order> rdd2 = rdd1.mapPartitions(new FlatMapFunction<Iterator<String>, Order>() {
            @Override
            public Iterator<Order> call(Iterator<String> partitionIter) throws Exception {
                Connection connection = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/doit50", "root", "ABC123.abc123");
                PreparedStatement stmt = connection.prepareStatement("select category,brand from product where pid= ? ");

                ArrayList<Order> orders = new ArrayList<>();
                while (partitionIter.hasNext()) {
                    String json = partitionIter.next();

                    Order order = JSON.parseObject(json, Order.class);

                    stmt.setInt(1, order.getPid());
                    ResultSet resultSet = stmt.executeQuery();
                    resultSet.next();
                    String category = resultSet.getString("category");
                    String brand = resultSet.getString("brand");

                    order.setCategory(category);
                    order.setBrand(brand);


                    orders.add(order);
                }


                return orders.iterator();
            }
        });

        // 按 "品牌+商品" 分组，聚合：单数、人数、金额
        JavaPairRDD<String, Order> brandAndPid_KVRdd = rdd2.mapToPair(od -> Tuple2.apply(od.getBrand() + "\001" + od.getPid(), od));
        JavaPairRDD<String, Agg> brandAndPid_Agg_KVRDD = brandAndPid_KVRdd.aggregateByKey(
                new Agg(),
                (agg, od) -> {
                    // 把收到的order订单数据，聚合到累加器
                    agg.orderCount++;
                    agg.sum += od.amt;
                    agg.userBitmap.add(od.uid);

                    // 让agg带上品牌和商品信息
                    agg.brand = od.brand;
                    agg.pid = od.pid;

                    return agg;
                },
                // agg和agg之间的聚合
                (agg1, agg2) -> {

                    agg1.orderCount += agg2.orderCount;
                    agg1.sum += agg2.sum;
                    agg1.userBitmap.or(agg2.userBitmap);

                    return agg1;
                }
        );


        // 让spark框架在分布式机制中，把数据按 “相同品牌中订单数大小”  倒序排序
        JavaPairRDD<Agg, Agg> agg_brandAndPid_KVRDD = brandAndPid_Agg_KVRDD.mapToPair(tp -> Tuple2.apply(tp._2, tp._2));// 把原来kv对调：把agg做key
        // 排序: 先比品牌，品牌相同则比订单数
        JavaPairRDD<Agg, Agg> sortedKVRDD = agg_brandAndPid_KVRDD.repartitionAndSortWithinPartitions(
                new BrandPartitioner(2),  // 分区：按品牌分
                new AggComparator() // 排序 ：先比品牌，品牌相同则比订单数
        );

        // sortedKVRDD: Tuple2(agg,品牌+商品)
        //  p0:  agg{b1,p1,100}=>b1+p1
        //       agg{b1,p3,80}=>b1+p3
        //       agg{b1,p4,70}=>b1+p4

        //       agg{b3,p6,120}
        //       agg{b3,p8,100}
        //       agg{b3,p7,90}
        //       agg{b3,p9,70}

        //
        //JavaPairRDD<String, Agg> brand_agg_KVRDD = sortedKVRDD.mapToPair(tp -> Tuple2.apply(tp._1.brand, tp._1));// key：品牌，  value：商品聚合结果agg
        //JavaPairRDD<Agg, Agg> agg_agg_KVRDD = sortedKVRDD.mapToPair(tp -> Tuple2.apply(tp._1, tp._1));

        // 组[ agg{b1,p1,100}=>agg{b1,p1,100} , agg{b1,p3,80}=>agg{b1,p3,80}, agg{b1,p4,70}=>agg{b1,p4,70}  ]
        // 为了让相同品牌的agg对象被分到同一组，需要重写agg的hashcode和equals
        JavaPairRDD<Agg, Iterable<Agg>> grouped = sortedKVRDD.groupByKey(new BrandPartitioner(2));

        JavaRDD<Agg> resRDD = grouped.flatMap(new FlatMapFunction<Tuple2<Agg, Iterable<Agg>>, Agg>() {
            @Override
            public Iterator<Agg> call(Tuple2<Agg, Iterable<Agg>> kv) throws Exception {
                Iterable<Agg> values = kv._2;

                ArrayList<Agg> aggs = new ArrayList<>();
                int i = 0;
                for (Agg value : values) {
                    aggs.add(value);
                    if (++i >= 2) break;
                }
                return aggs.iterator();
            }
        });

        resRDD.foreach(s-> System.out.println(s));


    }

    public static class AggComparator implements Comparator<Agg>,Serializable{

        @Override
        public int compare(Agg agg1, Agg agg2) {
            int tmp = agg1.brand.compareTo(agg2.brand);
            return tmp == 0 ? Integer.compare(agg2.orderCount, agg1.orderCount) : tmp;
        }
    }


    public static class BrandPartitioner extends HashPartitioner implements Serializable{
        int partitions;

        public BrandPartitioner(int partitions) {
            super(partitions);
            this.partitions = partitions;
        }

        @Override
        public int getPartition(Object key) {
            Agg agg = (Agg) key;
            return super.getPartition(agg.getBrand());
        }
    }


    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    @ToString
    @Data
    public static class Agg implements Serializable {
        private int pid;
        private RoaringBitmap userBitmap = RoaringBitmap.or();
        private int orderCount = 0;
        private double sum = 0;
        private int userCount;
        private String brand;

        public Agg(int uid, int orderCount, double sum) {
            this.orderCount = orderCount;
            this.sum = sum;
            this.userBitmap.add(uid);
        }


        // 重写hashcode：取决于品牌
        @Override
        public int hashCode() {
            return this.brand.hashCode();
        }


        // 重写equals：取决于品牌
        @Override
        public boolean equals(Object obj) {
            if(obj instanceof Agg){
                return ((Agg) obj).brand.equals(this.brand);
            }
            return false;
        }
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order implements Serializable {
        private int uid;
        private String oid;
        private int pid;
        private double amt;
        private String category;
        private String brand;
    }


}

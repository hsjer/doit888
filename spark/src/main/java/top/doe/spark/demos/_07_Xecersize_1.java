package top.doe.spark.demos;


import com.alibaba.fastjson.JSON;
import com.google.common.collect.Iterators;
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

public class _07_Xecersize_1 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.set("spark.default.parallelism", "2");
        conf.setAppName("name");


        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("./spark_data/excersize_3/input/order.data", 2);
        System.out.println(rdd1.getNumPartitions() + "----------------------------------");

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
        rdd2.cache();   // 将rdd2的结果数据物化并缓存在的集群的task执行器（executor）的内存中

        // 计算需求1
        JavaRDD<String> res1RDD = calculateRequirementOne(rdd2);
        //res1RDD.saveAsTextFile("./spark_data/order_output1/");

        // 计算需求3
        JavaRDD<String> res3RDD = calculateRequirementThree(rdd2);
        //res3RDD.saveAsTextFile("./spark_data/order_output3/");


        JavaRDD<String> res4RDD = calculateRequirementThree_2(rdd2);
        res4RDD.saveAsTextFile("./spark_data/order_output4/");

    }


    // 需求3的版本1 -- 按品牌为粒度去做
    private static JavaRDD<String> calculateRequirementThree(JavaRDD<Order> rdd2) {
        // 各品牌中，购买的订单数最多的前2种商品，及其购买订单数、人数、总金额
        return rdd2.mapToPair(od -> new Tuple2<>(od.getBrand(), od))
                .groupByKey()
                .flatMap(new FlatMapFunction<Tuple2<String, Iterable<Order>>, String>() {
                    @Override
                    public Iterator<String> call(Tuple2<String, Iterable<Order>> tp2) throws Exception {
                        String brand = tp2._1;
                        Iterable<Order> orders = tp2._2;

                        // 计算每种商品的购买单数、人数、金额
                        HashMap<Integer, AggThree> mp = new HashMap<>();

                        // 遍历处理这组订单的每一个订单
                        for (Order order : orders) {
                            int pid = order.getPid();
                            if (mp.containsKey(pid)) {
                                AggThree agg = mp.get(pid);
                                // 添加人数
                                agg.getUserBitmap().add(order.getUid());

                                // 添加单数
                                agg.orderCount++;

                                // 添加金额
                                agg.sum += order.getAmt();
                            } else {
                                mp.put(pid, new AggThree(order.getUid(), 1, order.getAmt()));
                            }
                        }


                        // 取订单数最大的前2个
                        AggThree first = null;
                        AggThree second = null;

                        for (Map.Entry<Integer, AggThree> entry : mp.entrySet()) {
                            Integer pid = entry.getKey();
                            AggThree agg = entry.getValue();
                            agg.setPid(pid);


                            if (first == null || agg.orderCount > first.orderCount) {
                                second = first;
                                first = agg;
                            } else if (second == null || agg.orderCount > second.orderCount) {
                                second = agg;
                            }
                        }


                        ArrayList<String> res = new ArrayList<>();
                        if (first != null) {
                            first.setUserCount(first.userBitmap.getCardinality());
                            first.setUserBitmap(null);

                            res.add(JSON.toJSONString(first));
                        }
                        ;
                        if (second != null) {

                            second.setUserCount(second.userBitmap.getCardinality());
                            second.setUserBitmap(null);

                            res.add(JSON.toJSONString(second));

                        }
                        ;

                        return res.iterator();
                    }
                });
    }


    // 需求3的版本2 -- 按商品为粒度去做
    private static JavaRDD<String> calculateRequirementThree_2(JavaRDD<Order> rdd2) {

        /* *
         * 先按 品牌+商品，计算出每个商品的 订单数、订单额、人数
         */
        // KEY: 品牌_商品
        // VALUE: 订单对象
        JavaPairRDD<String, Order> pair = rdd2.mapToPair(od -> new Tuple2<>(od.getBrand() + "\001" + od.getPid(), od));
        JavaPairRDD<String, AggThree> aggTmp1 = pair.aggregateByKey(
                new AggThree(),
                new Function2<AggThree, Order, AggThree>() {
                    @Override
                    public AggThree call(AggThree agg, Order od) throws Exception {
                        agg.userBitmap.add(od.getUid());
                        agg.orderCount++;
                        agg.sum += od.getAmt();

                        agg.setBrand(od.getBrand());
                        agg.setPid(od.getPid());

                        return agg;
                    }
                },
                new Function2<AggThree, AggThree, AggThree>() {
                    @Override
                    public AggThree call(AggThree agg1, AggThree agg2) throws Exception {
                        agg1.userBitmap.or(agg2.userBitmap);
                        agg1.orderCount += agg2.orderCount;
                        agg1.sum += agg2.sum;

                        return agg1;
                    }
                }
        );


        // 按 品牌分组
        JavaPairRDD<String, Iterable<Tuple2<String, AggThree>>> pair2 = aggTmp1.groupBy(new Function<Tuple2<String, AggThree>, String>() {
            @Override
            public String call(Tuple2<String, AggThree> kv) throws Exception {
                String preKey = kv._1;
                // 从原来的key中切出品牌，作为新的groupBy的key
                String brand = preKey.split("\001")[0];
                return brand;
            }
        });


        JavaRDD<AggThree> aggRes = pair2.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Tuple2<String, AggThree>>>, AggThree>() {
            @Override
            public Iterator<AggThree> call(Tuple2<String, Iterable<Tuple2<String, AggThree>>> tp2) throws Exception {

                // tp2._2是同一个品牌下的所有商品的计算结果
                Iterable<Tuple2<String, AggThree>> iter = tp2._2;

                TreeMap<AggThree, Object> treeMap = new TreeMap<>(
                        (agg1, agg2) -> {
                            int compare = Integer.compare(agg2.orderCount, agg1.orderCount);
                            return compare == 0 ? Integer.compare(agg1.pid, agg2.pid) : compare;
                        }
                );

                // kv:
                // k :  品牌\001商品ID
                // v :  一个商品的聚合值：单数，人数，金额

                // 迭代一组同品牌下的商品计算结果
                for (Tuple2<String, AggThree> kv : iter) {
                    AggThree agg = kv._2;
                    System.out.println(agg);
                    treeMap.put(agg, null);
                    if (treeMap.size() > 3) {
                        treeMap.pollLastEntry();  // 拉取并移除最后一条entry
                    }
                }

                return treeMap.keySet().iterator();
            }
        });

        return aggRes.map(JSON::toJSONString);
    }


    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    @ToString
    public static class AggThree implements Serializable {
        private int pid;
        private RoaringBitmap userBitmap = RoaringBitmap.or();
        private int orderCount = 0;
        private double sum = 0;
        private int userCount;
        private String brand;

        public AggThree(int uid, int orderCount, double sum) {
            this.orderCount = orderCount;
            this.sum = sum;
            this.userBitmap.add(uid);
        }


    }


    private static JavaRDD<String> calculateRequirementOne(JavaRDD<Order> rdd2) {
        //  各品类的=> 购买人数、购买总额，人均购买金额
        JavaPairRDD<String, Order> rdd3 = rdd2.mapToPair(o -> new Tuple2<>(o.getCategory(), o));

        JavaPairRDD<String, AggOne> rdd4 = rdd3.aggregateByKey(
                new AggOne(),
                new Function2<AggOne, Order, AggOne>() {
                    @Override
                    public AggOne call(AggOne agg, Order od) throws Exception {
                        agg.users.add(od.getUid());
                        agg.sum += od.getAmt();

                        return agg;
                    }
                },
                new Function2<AggOne, AggOne, AggOne>() {
                    @Override
                    public AggOne call(AggOne agg1, AggOne partial) throws Exception {
                        agg1.users.addAll(partial.users);
                        agg1.sum += partial.sum;
                        return agg1;
                    }
                }
        );

        // 变换成最终结果
        JavaRDD<String> resRDD = rdd4.map(new Function<Tuple2<String, AggOne>, String>() {
            @Override
            public String call(Tuple2<String, AggOne> tp) throws Exception {
                String category = tp._1;
                AggOne aggOne = tp._2;

                int userCount = aggOne.users.size();

                return category + " => " + userCount + "," + Math.round(aggOne.sum * 100) / 100.0 + "," + Math.round(aggOne.sum * 100 / userCount) / 100.0;
            }
        });
        return resRDD;
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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AggOne implements Serializable {
        private HashSet<Integer> users = new HashSet<>();  // 人数
        private double sum;  // 总金额
    }


}

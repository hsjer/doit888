package top.doe.spark.demos;


import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

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
 * 各品牌中，购买的订单数最多的前2种商品，及其购买订单数、人数、总金额
 **/

public class _07_Xecersize_1_refine {

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
                // 创建连接
                Connection connection = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/doit50", "root", "ABC123.abc123");
                PreparedStatement stmt = connection.prepareStatement("select category,brand from product where pid= ? ");

                // 利用输入迭代，造一个自己的返回处理结果的迭代器
                Iterator<Order> resIterator = new Iterator<Order>() {
                    @Override
                    public boolean hasNext() {
                        return partitionIter.hasNext();
                    }

                    @Override
                    public Order next() {
                        String json = partitionIter.next();
                        Order order = JSON.parseObject(json, Order.class);

                        try {
                            stmt.setInt(1, order.getPid());
                            ResultSet resultSet = stmt.executeQuery();
                            resultSet.next();
                            String category = resultSet.getString("category");
                            String brand = resultSet.getString("brand");

                            order.setCategory(category);
                            order.setBrand(brand);

                            // 判断分区中后续是否还有数据要处理
                            // 如果后续没有数据了，就可以安全地关闭连接
                            if (!partitionIter.hasNext()) {
                                stmt.close();
                                connection.close();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        return order;
                    }
                };



                // 如果在这里关连接，则就是在task使用迭代器之前就已经把链接关了
                // 那么我们这个迭代器就无法正常工作了，因为已经没有连接了
                // stmt.close();
                // connection.close();


                return resIterator;
            }
        });

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

        resRDD.foreach(s -> System.out.println(s));


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

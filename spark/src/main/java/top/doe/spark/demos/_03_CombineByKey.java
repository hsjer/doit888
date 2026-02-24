package top.doe.spark.demos;


import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/11
 * @Desc: 学大数据，上多易教育
 * 有如下数据：
 * {"uid":1,"oid":"o_1","pid":1,"amt":78.8}
 * {"uid":1,"oid":"o_2","pid":2,"amt":68.8}
 * {"uid":1,"oid":"o_3","pid":1,"amt":160.0}
 * {"uid":2,"oid":"o_4","pid":2,"amt":120.8}
 * {"uid":2,"oid":"o_5","pid":2,"amt":780.8}
 * {"uid":2,"oid":"o_6","pid":3,"amt":68.8}
 * {"uid":3,"oid":"o_7","pid":3,"amt":78.8}
 * {"uid":3,"oid":"o_8","pid":2,"amt":78.8}
 * <p>
 * 统计：
 * 每个商品的    订单数 和 订单总金额
 **/
public class _03_CombineByKey {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("name");


        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("./spark_data/excersize_3/input/order.data");

        // 解析原始数据，变成 KV对RDD
        JavaPairRDD<Integer, Order> rdd2 = rdd1.mapToPair(new PairFunction<String, Integer, Order>() {
            @Override
            public Tuple2<Integer, Order> call(String json) throws Exception {
                Order order = JSON.parseObject(json, Order.class);
                return Tuple2.apply(order.getPid(), order);
            }
        });

        // ---------用 combineByKey 来做分组聚合 ------
        rdd2.combineByKey(
                // 在做聚合时，一组数据的第一条数据会调这个方法，得到一个初始累加器（但不是0值累加器）
                new Function<Order, OrderAgg>() {
                    @Override
                    public OrderAgg call(Order od) throws Exception {
                        return new OrderAgg(1, od.getAmt());
                    }
                },

                // 来一条数据，如何聚合到累加器中
                new Function2<OrderAgg, Order, OrderAgg>() {
                    @Override
                    public OrderAgg call(OrderAgg agg, Order od) throws Exception {
                        agg.count += 1;
                        agg.sum += od.getAmt();
                        return agg;
                    }
                },

                // 对多个累加器结果做合并
                new Function2<OrderAgg, OrderAgg, OrderAgg>() {
                    @Override
                    public OrderAgg call(OrderAgg agg1, OrderAgg agg2) throws Exception {
                        agg1.count += agg2.count;
                        agg1.sum += agg2.sum;
                        return agg1;
                    }
                }
                ,new HashPartitioner(3),   // 可选参数：分区器
                false,   // 可选参数：是否做map端预聚合
                null  // 可选参数：自己的序列化器
        );


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


    // 订单数  和  订单总金额
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderAgg implements Serializable {
        private int count;
        private double sum;
    }
}

package top.doe.spark.demos;


import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/11
 * @Desc: 学大数据，上多易教育
 * <p>
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
 * 每个商品的  订单数  和  订单总金额
 **/
public class _02_Xecersize_3_aggregate_reduceByKey {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("name");


        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("./spark_data/excersize_3/input/order.data");

        // {"uid":3,"oid":"o_8","pid":2,"amt":78.8}
//        JavaPairRDD<Integer, Order> rdd2 = rdd1.mapToPair(new PairFunction<String, Integer, Order>() {
//            @Override
//            public Tuple2<Integer, Order> call(String json) throws Exception {
//                Order order = JSON.parseObject(json, Order.class);
//                int pid = order.getPid();
//                return Tuple2.apply(pid, order);
//            }
//        });

        JavaPairRDD<Integer, Order> rdd2 = rdd1.mapToPair(json -> {
            Order order = JSON.parseObject(json, Order.class);
            return new Tuple2<>(order.getPid(), order);
        });

        //------1. 用aggregateByKey - 累加器用 OrderAgg -------------
        JavaPairRDD<Integer, OrderAgg> res_1 = rdd2.aggregateByKey(
                new OrderAgg(),
                (agg, od) -> {
                    agg.count++;
                    agg.sum += od.getAmt();
                    return agg;
                },
                (agg, partialAgg) -> {
                    agg.count += partialAgg.count;
                    agg.sum += partialAgg.sum;
                    return agg;
                }
        );


        //------1. 用reduceByKey - 累加器用 OrderAgg -------------
        // 由于rdd2中的value类型是Order，而聚合的结果类型是OrderAgg，不一致，不能直接调用reduceByKey
        // api是死的，但人是活的，我们可以把rdd2先转换，把其中Order转成OrderAgg，这样，就可以用reduceByKey了
        JavaPairRDD<Integer, OrderAgg> rdd3 = rdd2.mapToPair(tp2 -> new Tuple2<>(tp2._1, new OrderAgg(1, tp2._2.getAmt())));
        JavaPairRDD<Integer, OrderAgg> res2 = rdd3.reduceByKey((agg1, agg2) -> {
            agg1.count += agg2.count;
            agg1.sum += agg2.sum;
            return agg1;
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


    // 订单数  和  订单总金额
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderAgg implements Serializable {
        private int count;
        private double sum;
    }

}

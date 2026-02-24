package top.doe.spark.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class _17_Collect_Count_Reduce {
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

        JavaRDD<Order> orderRDD = strRdd.map(s -> JSON.parseObject(s, Order.class));

        // 在task先按分布式正常aggregate聚合，然后把各task的聚合结果收集到driver端再聚合
        Double aggregate = orderRDD.aggregate(
                0.0,
                (agg, od) -> agg += od.amt,
                (agg1, agg2) -> agg1 += agg2
        );
        System.out.println(aggregate);


        // 需要输入的数据类型和累加器（也就是输出结果类型）一致
        JavaRDD<Double> doubleRDD = orderRDD.map(od -> od.amt);
        Double reduce = doubleRDD.reduce((d1, d2) -> d1 + d2);
        System.out.println(reduce);


        // fold可以指定初始值
        Double fold = doubleRDD
                .repartition(2)  // 把rdd变成2各分区
                .fold(10000.0, (d1, d2) -> d1 + d2);  // 分布式执行时聚合函数初始值，会被两个task并行度使用，到了driver端在使用一次
        System.out.println(fold);


        // 如果上面的reduce是做数字求和，那么可直接简化成调  sum算子
        JavaDoubleRDD javaDoubleRDD = orderRDD.mapToDouble(od -> od.amt);
        Double sum = javaDoubleRDD.sum();
        System.out.println(sum);


        // take 从rdd中取指定行数的数据到driver端
        List<String> take = strRdd.take(2);
        System.out.println(take);

        System.out.println("----------------------------");

        // top  : 底层调用的是 takeOrdered(com.reverse)
        List<Double> topAmt = doubleRDD.top(3);
        List<Order> topOrder = orderRDD.top(3, new AmtComparator(true));
        System.out.println(topAmt);
        System.out.println(topOrder);


        //
        List<Order> orders = orderRDD.takeOrdered(3, new AmtComparator(false));
        System.out.println(orders);


    }


    public static class AmtComparator implements Comparator<Order>, Serializable {
        boolean ascending;

        public AmtComparator(boolean ascending) {
            this.ascending = ascending;
        }


        @Override
        public int compare(Order o1, Order o2) {
            if(ascending) {
                return Double.compare(o1.amt, o2.amt);
            }else{
                return Double.compare(o2.amt, o1.amt);
            }
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
    }
}

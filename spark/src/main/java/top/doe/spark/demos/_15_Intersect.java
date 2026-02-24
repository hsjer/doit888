package top.doe.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class _15_Intersect {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("xxx");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // ------造一个rdd-------------------------------------
        List<Integer> lst1 = Arrays.asList(
               1,2,3,4,3,5,5,6
        );
        JavaRDD<Integer> rdd1 = sc.parallelize(lst1);


        // ------造一个rdd-------------------------------------
        List<Integer> lst2 = Arrays.asList(
                1,3,2,2
        );
        JavaRDD<Integer> rdd2 = sc.parallelize(lst2);

        JavaRDD<Integer> res = rdd1.intersection(rdd2);
        //res.foreach(s-> System.out.println(s));

        res.collect();

        rdd1.subtract(rdd2).foreach(s-> System.out.println(s));

    }
}

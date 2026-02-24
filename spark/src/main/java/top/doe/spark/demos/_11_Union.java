package top.doe.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.*;
import java.util.stream.StreamSupport;

public class _11_Union {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("name");


        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> lst1 = Arrays.asList(
                "a,1",
                "a,8",
                "a,10",
                "a,6",
                "b,10",
                "b,2",
                "b,20",
                "c,30",
                "c,50");

        // 需求，分组求和
        JavaRDD<String> rdd1 = sc.parallelize(lst1,3);




        List<String> lst2 = Arrays.asList(
                "x,1",
                "x,8",
                "x,10",
                "x,6",
                "y,10",
                "y,2",
                "y,20",
                "z,30",
                "z,50");

        JavaRDD<String> rdd2 = sc.parallelize(lst2, 3);


        JavaRDD<String> rdd3 = rdd1.union(rdd2);


        JavaRDD<String> res = rdd3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
                System.out.println("------------------当前处理的分区： " + v1);
                return v2;
            }
        }, true);


        res.foreach(s-> System.out.println(s));


    }






}

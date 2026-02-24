package top.doe.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class _12_Subtract {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("name");


        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> lst1 = Arrays.asList("a","b","c","d","a");
        JavaRDD<String> rdd1 = sc.parallelize(lst1,3);


        List<String> lst2 = Arrays.asList("a","c","x");
        JavaRDD<String> rdd2 = sc.parallelize(lst2, 3);


        // 广义上，subtract是需要shuffle的
        // 但是，跟reduceByKey，如果满足某个严苛条件，也可以不需要shuffle
        // 上游和结果rdd的分区器是HashPartitioner，且分区数与结果rdd的分区数形同
        // 准确又精炼的总结是： 数据不需要打乱重组，就能算出正确结果，那么就不需要shuffle
        JavaRDD<String> rdd3 = rdd1.subtract(rdd2);


        rdd3.foreach(s-> System.out.println(s));


    }






}

package top.doe.spark.demos;

import com.google.common.collect.Iterators;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class _05_GroupByKey {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.set("spark.default.parallelism","1");
        conf.setAppName("name");


        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> datas = Arrays.asList(
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
        JavaRDD<String> rdd1 = sc.parallelize(datas);

        JavaPairRDD<String, Integer> rdd2 =
                rdd1.mapToPair(s -> new Tuple2<String, Integer>(
                        s.split(",")[0],
                        Integer.parseInt(s.split(",")[1]))
                );

        // group by key  ：按照key分组
        // (a,[1,8,10,6])
        // (b,[10,2,20])
        // (c,[30,50])
        JavaPairRDD<String, Iterable<Integer>> rdd3 = rdd2.groupByKey();

        JavaRDD<Tuple2<String, Integer>> res = rdd3.map(tp -> {
            String key = tp._1;
            Iterable<Integer> iter = tp._2;
            int sum = 0;
            for (Integer v : iter) {
                sum += v;
            }

            return new Tuple2<String, Integer>(key, sum);
        });

    }

}

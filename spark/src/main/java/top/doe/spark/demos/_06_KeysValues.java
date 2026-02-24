package top.doe.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class _06_KeysValues {
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

        // keys ：取所有的key
        JavaRDD<String> keyRDD = rdd2.keys();

        // values：取所有的value
        JavaRDD<Integer> values = rdd2.values();

        // 对KV rdd的value映射
        JavaPairRDD<String, String> mapValues = rdd2.mapValues(v -> v + "_10");



    }
}

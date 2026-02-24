package top.doe.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class _01_WordCountWholeTextFile {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("xxx");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> rdd1 = sc.wholeTextFiles("./spark_data/wc/input/");
        JavaRDD<String> rdd2 = rdd1.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tp2) throws Exception {
                return tp2._2();
            }
        });

        rdd2.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.stream(line.split(" ")).iterator();
            }
        });




        JavaRDD<String> rddx = rdd1.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, String> tp2) throws Exception {
                // tp2._1 是文件名
                // tp2._2 是一行数据
                String line = tp2._2;
                return Arrays.stream(line.split(" ")).iterator();
            }
        });

        rdd2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String w) throws Exception {
                return Tuple2.apply(w,1);
            }
        });

    }
}

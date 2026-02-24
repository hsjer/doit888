package top.doe.spark.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.KryoSerializer;
import scala.Tuple2;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class _01_WordCount {

    public static void main(String[] args) throws InterruptedException {

        // 构造spark的参数对象用于设置参数
        SparkConf conf = new SparkConf();
        // mapreduce.framework.name=yarn/local
        // local[2] 代表用2个线程模拟分布式运行 local[*]模拟的线程数是机器的cpu线程数
        conf.setMaster("local");
        conf.setAppName("wordcount");
        //conf.set("spark.serializer", KryoSerializer.class.getName())
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");


        // 获取spark的编程入口 (JAVA)
        JavaSparkContext sc = new JavaSparkContext(conf);


        // 把文件映射成rdd [“集合”中的元素是文件中的一行一行的字符串]
        // [
        //    "a b c c"
        //    "b c d d"
        // ]
        JavaRDD<String> rdd1 = sc.textFile("spark_data/wordcount/input");

        JavaPairRDD<String, Integer> mapRdd = rdd1.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Iterable<Integer>> gpRdd = mapRdd.groupByKey();
        gpRdd.foreach(s-> System.out.println(s));
        Thread.sleep(Long.MAX_VALUE);


        // map代表一种运算模式：  一 一映射    rdd1元素 ->  rdd2元素
        //JavaRDD<String[]> rdd2 = rdd1.map(new LineSplit());

        // flatmap代表一种运算模式： 对进来的一行数据先映射成 集合或数组 ，然后把结果集合或者数组 压平 得到结果rdd中的元素
        // [
        //    "a"
        //    "a"
        //    "b"
        //    "c"
        // ]
        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {

                String[] wordsArray = s.split(" ");
                Thread.sleep(10000);

                ByteBuffer allocate = ByteBuffer.allocate(32 * 1024 * 1024);


                return Arrays.stream(wordsArray).iterator();
            }
        });



        // [
        //    ("a",1)
        //    ("a",1)
        //    ("b",1)
        //    ("c",1)
        // ]
//        JavaRDD<Tuple2<String, Integer>> rdd3 = rdd2.map(new Function<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> call(String word) throws Exception {
//                return new Tuple2<>(word, 1);
//            }
//        });


        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });


        // 分组聚合
        // [
        //    ("a",2)
        //    ("b",3)
        //    ("c",4)
        // ]
        JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override  // v1是此前的聚合结果   v2是一条新的数据
            public Integer call(Integer v1, Integer v2) throws Exception {

                Thread.sleep(10000);

                return v1 + v2;
            }
        });


        // 输出rdd的数据，就会触发计算任务的真正执行
        rdd4.saveAsTextFile(args[1]);


    }
}

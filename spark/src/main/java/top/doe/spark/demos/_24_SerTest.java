package top.doe.spark.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

public class _24_SerTest {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("wordcount");
        conf.setMaster("local");


        // 获取spark的编程入口 (JAVA)
        JavaSparkContext sc = new JavaSparkContext(conf);



        // {"uid":1,"oid":"o_1","pid":1,"amt":78.8}
        JavaRDD<String> rdd1 = sc.textFile("spark_data/excersize_3/input/");
        JavaRDD<Od> rdd2 = rdd1.map(s -> {
            Od od = JSON.parseObject(s, Od.class);
            return od;
        });

        //rdd2.persist(StorageLevel.MEMORY_ONLY_SER());
        rdd2.cache();  //等价于rdd2.persist(StorageLevel.MEMORY_ONLY()); od对象是不需要被序列化的

        // 输出rdd的数据，就会触发计算任务的真正执行
        rdd2.saveAsTextFile("spark_data/ser_output/");
        rdd2.map(od->od.amt).saveAsTextFile("spark_data/ser_output2/");




    }

    // {"uid":1,"oid":"o_1","pid":1,"amt":78.8}
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Od {
        private int uid;
        private String oid;
        private int pid;
        private double amt;
    }
}

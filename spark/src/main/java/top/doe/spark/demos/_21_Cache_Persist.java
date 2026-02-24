package top.doe.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import top.doe.utils.StreamUtils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class _21_Cache_Persist {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("xxx");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);


        List<String> lst = Arrays.asList("a","a","a","a","a","a","a","a","a","a","b","b","c","c");
        JavaRDD<String> rdd1 = sc.parallelize(lst, 2);

        // 假设rdd1在后续的逻辑中需要反复用到，则可以把它cache起来
        rdd1.cache();   // persist(StorageLevel.MEMORY_ONLY)
        rdd1.persist(StorageLevel.MEMORY_ONLY_2());  // 只放内存，且有两个副本

        rdd1.checkpoint(); // 会物化rdd，且物化结果写入HDFS文件系统


        rdd1.count();


    }

}

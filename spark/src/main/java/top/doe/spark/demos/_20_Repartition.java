package top.doe.spark.demos;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import top.doe.utils.StreamUtils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class _20_Repartition {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("xxx");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);


        List<String> lst = Arrays.asList("a","a","a","a","a","a","a","a","a","a","b","b","c","c");
        JavaRDD<String> rdd1 = sc.parallelize(lst, 2);

        // 打印分区情况
        rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer partitionId, Iterator<String> iter) throws Exception {
                System.out.println("-------当前分区:" + partitionId + " ----------------");
                StreamUtils.it2Stream(iter).forEach(i -> System.out.println(i));

                return iter;
            }
        }, true).collect();


        System.out.println("-------------------------华丽的分割线-----------------------------------");


        JavaRDD<String> rdd2 = rdd1.repartition(3);
        // 打印分区情况
//        rdd2.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
//            @Override
//            public Iterator<String> call(Integer partitionId, Iterator<String> iter) throws Exception {
//                System.out.println("-------当前分区:" + partitionId + " ----------------");
//                StreamUtils.it2Stream(iter).forEach(i -> System.out.println(i));
//
//                return iter;
//            }
//        }, true).collect();



        System.out.println("-------------coalesce------------华丽的分割线-----------------------------------");

        JavaRDD<String> rdd3 = rdd1.coalesce(4, true);
        // 打印分区情况
        rdd3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer partitionId, Iterator<String> iter) throws Exception {
                System.out.println("-------当前分区:" + partitionId + " ----------------");
                StreamUtils.it2Stream(iter).forEach(i -> System.out.println(i));

                return iter;
            }
        }, true).collect();


    }

}

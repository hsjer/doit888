package top.doe.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/11
 * @Desc: 学大数据，上多易教育
 **/
public class _02_Xecersize_2_reduceByKey {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("name");


        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> datas = Arrays.asList("a,1", "a,8", "a,10", "a,6", "b,10", "b,2", "b,20", "c,30", "c,50");

        // 需求，分组求和
        JavaRDD<String> rdd1 = sc.parallelize(datas);

        // 先转换成KV RDD
        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(s -> new Tuple2<>(s.split(",")[0], Integer.parseInt(s.split(",")[1])));

        //-----------------------------------------------------------------

        // 一、可以使用aggregateByKey来做 分组聚合
        JavaPairRDD<String, Integer> resRdd1 = rdd2.aggregateByKey(
                0,
                (agg, value) -> agg + value,   // 聚合数据到累加器  _+_
                (agg, partialAgg) -> agg + partialAgg // 聚合累加器到累加器
        );

        resRdd1.foreach(s -> System.out.println(s));


        //-----------------------------------------------------------------


        // 二、当满足如下简化场景条件时，也可以使用reduceByKey来做分组聚合：
        // 当使用aggregateByKey做聚合时，如果输入数据的value类型和聚合完的累加器类型，一致
        JavaPairRDD<String, Integer> resRdd2 = rdd2.reduceByKey(Integer::sum);
        resRdd2.foreach(s -> System.out.println(s));
    }

}

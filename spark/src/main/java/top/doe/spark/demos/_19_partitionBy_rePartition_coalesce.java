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

public class _19_partitionBy_rePartition_coalesce {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("xxx");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);


        List<Integer> lst = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        JavaRDD<Integer> rdd1 = sc.parallelize(lst, 2);
        rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
            @Override
            public Iterator<Integer> call(Integer partitionId, Iterator<Integer> iter) throws Exception {
                System.out.println("-------当前分区:" + partitionId + " ----------------");

                StreamUtils.it2Stream(iter).forEach(i -> System.out.println(i));
                return iter;
            }
        }, true).collect();


        System.out.println("---------------------------");

        // 重新分区
        JavaPairRDD<String, Integer> kvRdd = rdd1.mapToPair(i -> Tuple2.apply(i + "", i));
        JavaPairRDD<String, Integer> partitionByRdd = kvRdd.partitionBy(new HashPartitioner(2) {
            @Override
            public int getPartition(Object key) {
                String intStr = (String) key;
                int k = Integer.parseInt(intStr);

                if (k % 2 == 0) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });



        partitionByRdd.mapPartitionsWithIndex(
                new Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<Tuple2<String,Integer>>>() {
                    @Override
                    public Iterator<Tuple2<String,Integer>> call(Integer v1, Iterator<Tuple2<String, Integer>> v2) throws Exception {
                        System.out.println("-------当前分区: " + v1 + "-------------");
                        while(v2.hasNext()){
                            Tuple2<String, Integer> next = v2.next();
                            System.out.println(next);
                        }
                        return v2;
                    }
                }
                , true
        ).collect();

    }

}

package top.doe.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class _10_MapPartitionsWithIndex {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
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
        JavaRDD<String> rdd1 = sc.parallelize(datas,3);

//        rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String[]>>() {
//            @Override
//            public Iterator<String[]> call(Integer partitionId, Iterator<String> partitionIterator) throws Exception {
//                return new MyIterator(partitionIterator);
//            }
//        },true);


        JavaRDD<String[]> resRDD = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String[]>>() {
            @Override
            public Iterator<String[]> call(Integer partitionId, Iterator<String> partitionIterator) throws Exception {

                System.out.println("--------当前的处理的分区是: " + partitionId + " ------------------");

                // 把迭代器变成JDK8新特性中的stream
                 return StreamSupport
                        .stream(Spliterators.spliteratorUnknownSize(partitionIterator, Spliterator.ORDERED), false)
                        .map(line -> line.split(","))
                        .iterator();

            }
        }, true);


        resRDD.foreach(s-> System.out.println(s));


    }




    public static class MyIterator implements Iterator<String[]> {
        Iterator<String> partitionIterator;

        public MyIterator(Iterator<String> partitionIterator) {
            this.partitionIterator = partitionIterator;
        }


        @Override
        public boolean hasNext() {
            return partitionIterator.hasNext();
        }

        @Override
        public String[] next() {
            String line = partitionIterator.next();
            String[] split = line.split(",");


            return split;
        }
    }


}

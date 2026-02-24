package top.doe.spark.demos;

import lombok.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class _09_SortBy_SortByKey {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("distinct_demo");
        conf.setMaster("local");
        conf.set("spark.default.parallelism", "2");

        JavaSparkContext sc = new JavaSparkContext(conf);


        // 对自定义类型去重
        // 需要重写hashcode：让对象成员值相同的返回相同的hashcode
        List<Person> lst = Arrays.asList(
                new Person("hh", "bb", 28),
                new Person("ii", "aa", 18),
                new Person("kk", "bb", 28),
                new Person("kk", "aa", 18),
                new Person("ii", "cc", 38),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 27),
                new Person("hh", "dd", 32)
        );

        // hh228
        // hh228


        JavaRDD<Person> rdd = sc.parallelize(lst);

//        // sortBy所用partitioner，是rangePartitioner
//        // sortBy底层先把rdd变成 KV RDD,再调sortByKey
//        JavaRDD<Person> sorted = rdd.sortBy(p -> p.getGender(), true, 2);
//        sorted.saveAsTextFile("./spark_data/sortBy/");
//
//        JavaPairRDD<String, Person> kvRDD = rdd.keyBy(p -> p.gender);
//        JavaPairRDD<String, Person> sorted2 = kvRDD.sortByKey();
//        sorted2.saveAsTextFile("./spark_data/sortBy2/");


        // 对复杂对象进行全局排序: 先比性别，再比年龄
        JavaPairRDD<Person, Person> kv = rdd.keyBy(p -> p);
        JavaPairRDD<Person, Person> sorted3 = kv.sortByKey(
               new MyComp(), true, 2);

        sorted3.keys().saveAsTextFile("./spark_data/sortBy3/");


    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @Data  // 小辣椒生成的hashcode，是利用对象中成员变量值生成；如果成员变量值相同，则两对象的hashcode就相同
    public static class Person implements Serializable {
        private String gender;
        private String name;
        private int age;
    }


    public static class MyComp implements Comparator<Person>,Serializable {

        @Override
        public int compare(Person p1, Person p2) {
            int tmp = p1.gender.compareTo(p2.gender);
            return tmp == 0 ? Integer.compare(p2.age, p1.age) : tmp;
        }
    }


}

package top.doe.spark.demos;

import lombok.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class _08_Distinct {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("distinct_demo");
        conf.setMaster("local");
        conf.set("spark.default.parallelism","2");


        JavaSparkContext sc = new JavaSparkContext(conf);

        // 对基本类型去重
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 3, 5, 1, 3, 3, 6));
        JavaRDD<Integer> distinct1 = rdd1.distinct();



        // 对自定义类型去重
        // 需要重写hashcode：让对象成员值相同的返回相同的hashcode
        List<Person> lst = Arrays.asList(
                new Person("aa", 18),
                new Person("bb", 28),
                new Person("cc", 38),
                new Person("aa", 18),
                new Person("bb", 28),
                new Person("dd", 38)
        );
        JavaRDD<Person> rdd = sc.parallelize(lst);
        JavaRDD<Person> distinct = rdd.distinct();

        distinct.foreach(e-> System.out.println(e));

    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @Data  // 小辣椒生成的hashcode，是利用对象中成员变量值生成；如果成员变量值相同，则两对象的hashcode就相同
    public static class Person implements Serializable {
        private String name;
        private int age;
    }




}

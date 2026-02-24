package top.doe.spark.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/23
 * @Desc: 学大数据，上多易教育
 *   shuffle算子不shuffle的举例
 **/
public class _26_ShuffleOperatorBuShuffle {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf();
        conf.setAppName("xxx");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // ------造一个rdd-------------------------------------
        List<Student> lst1 = Arrays.asList(
                new Student(1, "aa", 18),
                new Student(2, "bb", 28),
                new Student(3, "cc", 38),
                new Student(4, "dd", 26)
        );
        JavaRDD<Student> rdd1 = sc.parallelize(lst1);


        // ------造一个rdd--------------------------------------
        List<Score> lst2 = Arrays.asList(
                new Score(1, "c1", 180),
                new Score(1, "c2", 108),
                new Score(1, "c3", 98),
                new Score(2, "c1", 128),
                new Score(2, "c2", 238),
                new Score(2, "c4", 380),
                new Score(3, "c1", 308),
                new Score(3, "c3", 266),
                new Score(3, "c4", 244)
        );
        JavaRDD<Score> rdd2 = sc.parallelize(lst2);


        // 把两个rdd变成kvRDD
        JavaPairRDD<Integer, Student> kvRdd1 = rdd1.keyBy(stu -> stu.id);
        JavaPairRDD<Integer, Score> kvRdd2 = rdd2.keyBy(sco -> sco.sid);


        // 在需要分区的算子中，传入了分区个数
        // 则底层会创建一个 new HashPartitioner(numPartitions)
        kvRdd1.reduceByKey((x,y)->x,4);

        // 在需要分区的算子中，传入了分区器，则使用的就是传入的分区器
        kvRdd1.reduceByKey(new HashPartitioner(5),(x,y)->x);

        // 在需要分区的算子中,分区数或分区器都不传
        //
        kvRdd1.reduceByKey((x,y)->x);


        JavaPairRDD<Integer, Tuple2<Student, Score>> joined = kvRdd1.join(kvRdd2);
        // 变得更好看
        JavaRDD<Joined> goodLook = joined.map(tp -> {
            Student s = tp._2._1;
            Score c = tp._2._2;
            return new Joined(s.id, s.name, s.age, c.sid, c.courseName, c.score);
        });

        goodLook.foreach(s -> System.out.println(s));



        // ----------------------------
        JavaPairRDD<Integer, Student> p1 = kvRdd1.partitionBy(new HashPartitioner(4) {
            @Override
            public int getPartition(Object key) {
                return super.getPartition(key);
            }
        });

        JavaPairRDD<Integer, Score> p2 = kvRdd2.partitionBy(new HashPartitioner(4));

        JavaPairRDD<Integer, Tuple2<Student, Score>> pjoin = p1.join(p2);
        pjoin.foreach(s-> System.out.println(s));




        Thread.sleep(Long.MAX_VALUE);


    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student implements Serializable {
        private int id;
        private String name;
        private int age;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Score implements Serializable {
        private int sid;
        private String courseName;
        private double score;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Joined implements Serializable {
        private Integer id;
        private String name;
        private Integer age;
        private Integer sid;
        private String courseName;
        private Double score;
    }

}

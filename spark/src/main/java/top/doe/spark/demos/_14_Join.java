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

public class _14_Join {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("xxx");
        conf.setMaster("local");
        //conf.set("spark.default.parallelism","8");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // ------造一个rdd-------------------------------------
        List<Student> lst1 = Arrays.asList(
                new Student(1, "aa", 18),
                new Student(2, "bb", 28),
                new Student(3, "cc", 38),
                new Student(4, "dd", 26)
        );
        JavaRDD<Student> rdd1 = sc.parallelize(lst1,100);


        // ------造一个rdd--------------------------------------
        List<Scores> lst2 = Arrays.asList(
                new Scores(1, "c1", 180),
                new Scores(1, "c2", 108),
                new Scores(1, "c3", 98),
                new Scores(2, "c1", 128),
                new Scores(2, "c2", 238),
                new Scores(2, "c4", 380),
                new Scores(3, "c1", 308),
                new Scores(3, "c3", 266),
                new Scores(3, "c4", 244)
        );
        JavaRDD<Scores> rdd2 = sc.parallelize(lst2,800);


        // 把两个rdd变成kvRDD
        JavaPairRDD<Integer, Student> kvRdd1 = rdd1.keyBy(stu -> stu.id);
        JavaPairRDD<Integer, Scores> kvRdd2 = rdd2.keyBy(sco -> sco.sid);




        JavaPairRDD<Integer, Student> hasPartitioner = kvRdd1.partitionBy(new HashPartitioner(10));


        JavaPairRDD<Integer, Tuple2<Student, Scores>> joined = hasPartitioner.join(kvRdd2);
        Partitioner partitioner = joined.partitioner().get();
        int numPartitions = joined.getNumPartitions();
        System.out.println(partitioner);   // hashPartitioner
        System.out.println(numPartitions);  // 800


        System.exit(1);

        // 变得更好看
        JavaRDD<Joined> goodLook = joined.map(tp -> {
            Student s = tp._2._1;
            Scores c = tp._2._2;
            return new Joined(s.id, s.name, s.age, c.sid, c.courseName, c.score);
        });

        //goodLook.foreach(s -> System.out.println(s));


        // ------------------下面是leftOuterJoin---------------------------------------
        JavaPairRDD<Integer, Tuple2<Student, Optional<Scores>>> leftJoined = kvRdd1.leftOuterJoin(kvRdd2);
        JavaRDD<Joined> goodLookLeft = leftJoined.map(tp -> {
            Student s = tp._2._1;
            Optional<Scores> cOption = tp._2._2;

            if (cOption.isPresent()) {
                Scores c = cOption.get();
                return new Joined(s.id, s.name, s.age, c.sid, c.courseName, c.score);
            } else {
                return new Joined(s.id, s.name, s.age, null, null, null);
            }
        });
        goodLookLeft.foreach(s-> System.out.println(s));


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
    public static class Scores implements Serializable {
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

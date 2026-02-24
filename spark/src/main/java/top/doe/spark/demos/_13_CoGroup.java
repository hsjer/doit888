package top.doe.spark.demos;

import com.google.common.collect.Iterators;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static top.doe.utils.StreamUtils.*;

public class _13_CoGroup {
    public static void main(String[] args) {

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
        JavaRDD<Scores> rdd2 = sc.parallelize(lst2);


        // 把两个rdd变成kvRDD
        JavaPairRDD<Integer, Student> kvRdd1 = rdd1.keyBy(stu -> stu.id);
        JavaPairRDD<Integer, Scores> kvRdd2 = rdd2.keyBy(sco -> sco.sid);


        // 协同分组:必须针对kvRDD
        JavaPairRDD<Integer, Tuple2<Iterable<Student>, Iterable<Scores>>> coGrouped = kvRdd1.cogroup(kvRdd2);

        /* *
         * 4 => [Student(id=4, name=dd, age=26)] || []
         * 1 => [Student(id=1, name=aa, age=18)] || [Scores(sid=1, courseName=c1, score=180.0), Scores(sid=1, courseName=c2, score=108.0), Scores(sid=1, courseName=c3, score=98.0)]
         * 3 => [Student(id=3, name=cc, age=38)] || [Scores(sid=3, courseName=c1, score=308.0), Scores(sid=3, courseName=c3, score=266.0), Scores(sid=3, courseName=c4, score=244.0)]
         * 2 => [Student(id=2, name=bb, age=28)] || [Scores(sid=2, courseName=c1, score=128.0), Scores(sid=2, courseName=c2, score=238.0), Scores(sid=2, courseName=c4, score=380.0)]
         */
//        coGrouped.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<Student>, Iterable<Scores>>>>() {
//            @Override
//            public void call(Tuple2<Integer, Tuple2<Iterable<Student>, Iterable<Scores>>> tp) throws Exception {
//                Integer coKey = tp._1;
//                Iterable<Student> stuGroup = tp._2._1;
//                Iterable<Scores> scoGroup = tp._2._2;
//                Student[] stuArray = Iterators.toArray(stuGroup.iterator(), Student.class);
//                Scores[] scoArray = Iterators.toArray(scoGroup.iterator(), Scores.class);
//
//                System.out.println(coKey + " => " + Arrays.asList(stuArray) + " || " + Arrays.asList(scoArray));
//            }
//        });


        // 实现学生表 和 成绩表的 关联
        // 一、用基本java功能来实现；效率较低，因为我们真的从spark给的迭代器中取数据落地，且还把数据缓存在内存中了
        JavaRDD<Joined> joinedRdd1 = coGrouped.flatMap(new FlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Student>, Iterable<Scores>>>, Joined>() {
            @Override
            public Iterator<Joined> call(Tuple2<Integer, Tuple2<Iterable<Student>, Iterable<Scores>>> in) throws Exception {
                Iterable<Student> students = in._2._1;
                Iterable<Scores> scores = in._2._2;

                ArrayList<Joined> joineds = new ArrayList<>();
                for (Student s : students) {
                    for (Scores c : scores) {
                        joineds.add(new Joined(s.id, s.name, s.age, c.sid, c.courseName, c.score));
                    }
                }

                return joineds.iterator();
            }
        });


        // 二、用jdk8的stream新特性来实现，这样就只需要安排逻辑，并不会真的迭代数据落地
        JavaRDD<Joined> joinedRdd2 = coGrouped.flatMap(new FlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Student>, Iterable<Scores>>>, Joined>() {
            @Override
            public Iterator<Joined> call(Tuple2<Integer, Tuple2<Iterable<Student>, Iterable<Scores>>> in) throws Exception {
                Iterable<Student> students = in._2._1;
                Iterable<Scores> scores = in._2._2;

                Stream<Student> s1 = it2Stream(students);
                Stream<Scores> s2 = it2Stream(scores);

                // 相当于外层循环
                Stream<Joined> res = s1.flatMap(new Function<Student, Stream<Joined>>() {
                    @Override
                    public Stream<Joined> apply(Student s) {

                        // 相当于内层循环
                        Stream<Joined> joinedStream = s2.map(new Function<Scores, Joined>() {
                            @Override
                            public Joined apply(Scores c) {

                                return new Joined(s.id, s.name, s.age, c.sid, c.courseName, c.score);
                            }
                        });
                        return joinedStream;

                    }
                });

                return res.iterator();
            }
        });


        // 二、上面代码的lambda表达式简化写法
        JavaRDD<Joined> joinedRdd3 = coGrouped.flatMap(new FlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Student>, Iterable<Scores>>>, Joined>() {
            @Override
            public Iterator<Joined> call(Tuple2<Integer, Tuple2<Iterable<Student>, Iterable<Scores>>> in) throws Exception {
                return  it2Stream(in._2._1).flatMap(s-> it2Stream(in._2._2).map(c-> new Joined(s.id,s.name,s.age,c.sid,c.courseName,c.score))).iterator();
            }
        });

        joinedRdd3.foreach(s-> System.out.println(s));

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
        private int id;
        private String name;
        private int age;
        private int sid;
        private String courseName;
        private double score;
    }

}

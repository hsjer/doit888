package top.doe.spark.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class _16_SaveAsNewApiHadoopFile {
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

        // KV rdd 可以存为sequenceFile
        JavaPairRDD<IntWritable, Student> kvRDD = rdd1.mapToPair(new PairFunction<Student, IntWritable, Student>() {
            @Override
            public Tuple2<IntWritable, Student> call(Student student) throws Exception {
                IntWritable key = new IntWritable(student.id);
                return Tuple2.apply(key, student);
            }
        });


        // 调用hadoop的outputFormat去输出数据
        kvRDD.saveAsNewAPIHadoopFile("./spark_data/sequence_output/", IntWritable.class,Student.class, SequenceFileOutputFormat.class);


        // 调用hadoop的inputFormat去读数据
        JavaPairRDD<IntWritable, Student> rddSeq =
                sc.newAPIHadoopFile(
                        "./spark_data/sequence_output/",
                        SequenceFileInputFormat.class,
                        IntWritable.class,
                        Student.class,
                        new Configuration());

        rddSeq.values().foreach(s-> System.out.println(s));





    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student implements Writable,Serializable {
        private int id;
        private String name;
        private int age;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(id);
            out.writeUTF(name);
            out.writeInt(age);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.id = in.readInt();
            this.name = in.readUTF();
            this.age = in.readInt();

        }
    }

}

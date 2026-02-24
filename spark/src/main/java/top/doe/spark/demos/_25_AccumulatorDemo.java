package top.doe.spark.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Objects;

public class _25_AccumulatorDemo {
    public static void main(String[] args) {



        SparkConf conf = new SparkConf();
        conf.setAppName("wordcount");
        conf.setMaster("local");


        // 获取spark的编程入口 (JAVA)
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 获取一个累加器
        LongAccumulator accumulator = sc.sc().longAccumulator("dirty_lines");


        // {"uid":1,"oid":"o_1","pid":1,"amt":78.8}
        JavaRDD<String> rdd1 = sc.textFile("spark_data/excersize_3/input/");
        JavaRDD<Od> rdd2 = rdd1.map(s -> {

            Od od = null;
            try {
                od = JSON.parseObject(s, Od.class);
                // 用累加器对错误的json计数
                if (od == null) {
                    accumulator.add(1);
                }

            }catch (Exception e){
                accumulator.add(1);
            }
            return od;
        });


        rdd2.filter(Objects::nonNull).saveAsTextFile("spark_data/accumulate/");

        // job执行完后，可以通过累加器变量获取值
        long errLinesSum = accumulator.sum();   // 求累加器中的元素之和
        long errLineCount = accumulator.count(); // 取累加器中的元素个数

        System.out.println(errLinesSum);
        System.out.println(errLineCount);


    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Od {
        private int uid;
        private String oid;
        private int pid;
        private double amt;
    }
}

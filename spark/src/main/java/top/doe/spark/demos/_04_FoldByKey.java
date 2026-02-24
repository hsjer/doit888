package top.doe.spark.demos;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class _04_FoldByKey {
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
        JavaRDD<String> rdd1 = sc.parallelize(datas);

        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(s -> new Tuple2<String, Integer>(
                        s.split(",")[0],
                        Integer.parseInt(s.split(",")[1])
                )
        );

        //rdd2.foldByKey(100, (x,y)->x+y);
        //rdd2.reduceByKey(Integer::sum);
        JavaPairRDD<String, Integer> res = rdd2.foldByKey(100, Integer::sum);




        // 静态方法引用
        //res.foreach(MyUtil::println); // task拿去用的时候  tp -> MyUtil.println(tp)


        MyPrinter out = new MyPrinter();
        // 成员方法引用
        res.foreach(out::println);  // task在使用时： tp ->  out.println(tp)


        // 这样是行不通的，因为System.out这个对象不能被序列化
        //res.foreach(System.out::println);

        res.foreach(s-> System.out.println(s));


    }


    public static class MyUtil implements Serializable {
        public static void println(Tuple2<String,Integer> tp){
            System.out.println(tp);
        }
    }



    public static class MyPrinter implements Serializable {

        String aa = "hahaha";

        public void println(Tuple2<String,Integer> tp){
            System.out.println(this.aa + " => " + tp);
        }
    }


}

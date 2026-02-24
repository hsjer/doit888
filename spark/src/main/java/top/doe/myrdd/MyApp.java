package top.doe.myrdd;

import java.util.Arrays;

public class MyApp {
    public static void main(String[] args) throws Exception {

        MyRDD rdd = MyRDD.textFile("./spark_data/myrdd/a.txt");


        MyRDD rdd2 = rdd.filter(s -> s.startsWith("a"));


        MyRDD rdd3 = rdd2.map(s -> Arrays.toString(s.split(" ")));

        rdd3.saveAsTextFile("./spark_data/myrdd/output");


    }
}

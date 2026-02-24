package top.doe.spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.HashMap;

public class _23_SingletonThreadSafe {
    public static void main(String[] args) {


        // 构造spark的参数对象用于设置参数
        SparkConf conf = new SparkConf();
        conf.setAppName("wordcount");
        conf.setMaster("local");


        // 获取spark的编程入口 (JAVA)
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.textFile(args[0]);


        /* *
         *  线程安全示例1：广播了一个对象，然后在task还要往对象中写入数据
         */
        Broadcast<HashMap<String, Integer>> bc1 = sc.broadcast(new HashMap<String, Integer>());
        rdd1.map(s -> {

            HashMap<String, Integer> myCounter = bc1.value();


            // 广播出来的hashmap，在一个executor中只有一份
            // 而一个executor可能有多个task并行度在同时运行，而运行逻辑又往hashmap中写数据
            // 产生了线程安全问题
            String[] split = s.split(",");
            for (String w : split) {
                if (myCounter.containsKey(w)) {
                    Integer cnt = myCounter.get(w);
                    myCounter.put(w, cnt + 1);
                } else {
                    myCounter.put(w, 1);
                }

            }

            return "";

        });




        /* *
         *  线程安全示例2：广播了一个工具，工具创建的需要用于写的数据结构是一个单例的
         */

        // 广播counter工具
        Broadcast<MyCounterUtil> bc2 = sc.broadcast(new MyCounterUtil());

        rdd1.map(s -> {

            String[] split = s.split(",");
            // 广播变量
            MyCounterUtil myCounterUtil = bc2.value();

            // counter的是单例的,那么虽然getCounter()是每个task单独调用的，但是得到的counter是executor中唯一的
            // 而一个executor中可能有多个task并行度在同时运行，而运行逻辑中都会对counter做写操作
            // 就产生了线程安全问题
            HashMap<String, Integer> myCounter = myCounterUtil.getCounter();

            for (String w : split) {
                if (myCounter.containsKey(w)) {
                    Integer cnt = myCounter.get(w);
                    myCounter.put(w, cnt + 1);
                } else {
                    myCounter.put(w, 1);
                }

            }

            return "";
        });




        /* *
         *  线程安全示例3：闭包引用了一个对象，然后task要往闭包引用的对象进行写操作
         *  在这种情境下，并不会有线程安全问题：
         *      因为，闭包引用的对象，会随同task一起序列化，发给executor去反序列化再执行
         *      那么，每个task对象中都持有一个独立的hashmap对象
         */
        HashMap<String, Integer> myCounter = new HashMap<>();
        rdd1.map(s -> {
            String[] split = s.split(",");
            for (String w : split) {
                if (myCounter.containsKey(w)) {
                    Integer cnt = myCounter.get(w);
                    myCounter.put(w, cnt + 1);
                } else {
                    myCounter.put(w, 1);
                }

            }
            return "";
        });


        /* *
         *  线程安全示例 4：闭包引用了一个工具，然后又用这个闭包引用的工具获取一个静态的单例数据结构
         *  需要往单例数据结构里面写数据
         *  产生多个task并行度同时写一个对象的线程安全问题
         */
        rdd1.map(s -> {
            HashMap<String, Integer> counter = MyCounterUtil2.getCounter();

            String[] split = s.split(",");
            for (String w : split) {
                if (counter.containsKey(w)) {
                    Integer cnt = counter.get(w);
                    counter.put(w, cnt + 1);
                } else {
                    counter.put(w, 1);
                }

            }
            return "";
        });


    }

    public static class MyCounterUtil {
        HashMap<String, Integer> myCounter;

        public HashMap<String, Integer> getCounter() {
            if (this.myCounter == null) {
                this.myCounter = new HashMap<>();

            }
            return this.myCounter;
        }

    }


    public static class MyCounterUtil2 {
        static HashMap<String, Integer> myCounter;

        public static HashMap<String, Integer> getCounter() {
            if (myCounter == null) {
                myCounter = new HashMap<>();

            }
            return myCounter;
        }

    }


}

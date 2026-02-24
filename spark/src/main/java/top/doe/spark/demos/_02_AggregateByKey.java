package top.doe.spark.demos;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/10
 * @Desc: 学大数据，上多易教育
 * <p>
 * 输入数据：
 * {"id":1,"name":"aa",courses:[{"name":"dolphin","score":88.0},{"name":"java","score":80.0}, {"name":"python","score":99.0}]}
 * {"id":2,"name":"bb",courses:[{"name":"redis","score":86.0},{"name":"spark","score":90.0},{"name":"flink","score":80.0}]}
 * {"id":3,"name":"cc",courses:[{"name":"hbase","score":88.0},{"name":"hive","score":80.0}, {"name":"kafka","score":92.0}]}
 * {"id":4,"name":"dd",courses:[{"name":"doris","score":88.0},{"name":"java","score":82.0}, {"name":"python","score":85.0}]}
 * {"id":5,"name":"ee",courses:[{"name":"dolphin","score":88.0},{"name":"hive","score":81.0}, {"name":"flume","score":98.0}]}
 * <p>
 * 求：每门课程的（考试人数，总分）
 * java => 2, 176.5
 * python => 2, 180.6
 *
 *
 * 求：每门课程的（考试人数，总分，最高分，最低分，平均分）
 *
 **/

public class _02_AggregateByKey {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("ex3");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("./spark_data/excersize_2/input/some.data");

        // 1.先把数据加工成  KV形式
        // key: 是课程名
        // value: 是一个对象（用户id，用户名称，课程名，分数）
        JavaPairRDD<String, CourseInfo> rdd2 = rdd1.flatMapToPair(new PairFlatMapFunction<String, String, CourseInfo>() {
            @Override
            public Iterator<Tuple2<String, CourseInfo>> call(String s) throws Exception {

                // {"id":1,"name":"aa",courses:[{"name":"dolphin","score":88.0},{"name":"java","score":80.0}, {"name":"python","score":99.0}]}
                JSONObject jsonObject = JSON.parseObject(s);
                int id = jsonObject.getIntValue("id");
                String name = jsonObject.getString("name");
                JSONArray array = jsonObject.getJSONArray("courses");

                ArrayList<Tuple2<String, CourseInfo>> lst = new ArrayList<>();
                for (int i = 0; i < array.size(); i++) {
                    JSONObject obj = array.getJSONObject(i);
                    String courseName = obj.getString("name");
                    double score = obj.getDoubleValue("score");

                    // 产生一对K V
                    lst.add(new Tuple2<>(courseName, new CourseInfo(id, name, courseName, score)));
                }

                return lst.iterator();
            }
        });


        // 2. 按key做分组聚合
        // xxxByKey() 这种算子，在PairRDD上才有
        JavaPairRDD<String, Agg> rdd3 = rdd2.aggregateByKey(
                // 创建一个初始累加器的逻辑
                new Agg(),   // 1

                // 聚合数据的聚合逻辑 ( 如果有预聚合，则是shuffle的上游会调；如无，则是下游在调）
                new Function2<Agg, CourseInfo, Agg>() {   // 2
                    @Override
                    public Agg call(Agg agg, CourseInfo v2) throws Exception {

                        double score = v2.getScore();
                        agg.sum += score;
                        agg.count += 1;

                        return agg;
                    }
                },

                // 聚合累加的聚合逻辑  ( 如果有预聚合，则是shuffle的下游会调；如无，则没人调这个函数）
                new Function2<Agg, Agg, Agg>() {
                    @Override   // agg_new是新来的一个累加器，左边的agg是一个不断承受累加的
                    public Agg call(Agg agg, Agg agg_new) throws Exception {

                        agg.sum += agg_new.sum;
                        agg.count += agg_new.count;

                        return agg;
                    }
                }

        );

        // 把聚合结果变形成：java => 2, 176.5
        JavaRDD<String> rdd4 = rdd3.map(new Function<Tuple2<String, Agg>, String>() {
            @Override
            public String call(Tuple2<String, Agg> tp) throws Exception {
                String courseName = tp._1;
                Agg agg = tp._2;

                return courseName + " => " + agg.count + "," + agg.sum;
            }
        });

        // 输出最终结果
        rdd4.saveAsTextFile("./spark_data/aggregate_output");



    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CourseInfo implements Serializable {
        private int id;
        private String name;
        private String courseName;
        private double score;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Agg  implements Serializable {
        private int count;
        private double sum;

    }

}

package top.doe.spark.demos;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/10
 * @Desc: 学大数据，上多易教育
 *   输入数据：
{"id":1,"name":"aa",courses:[{"name":"dolphin","score":88.0},{"name":"java","score":80.0}, {"name":"python","score":99.0}]}
{"id":2,"name":"bb",courses:[{"name":"redis","score":86.0},{"name":"spark","score":90.0},{"name":"flink","score":80.0}]}
{"id":3,"name":"cc",courses:[{"name":"hbase","score":88.0},{"name":"hive","score":80.0}, {"name":"kafka","score":92.0}]}
{"id":4,"name":"dd",courses:[{"name":"doris","score":88.0},{"name":"java","score":82.0}, {"name":"python","score":85.0}]}
{"id":5,"name":"ee",courses:[{"name":"dolphin","score":88.0},{"name":"hive","score":81.0}, {"name":"flume","score":98.0}]}

 *
 *   需要加工成：每个人的课程中分数最好的前两门课程，并且每门课程的信息作为单独的一行
 *   1,aa,python,99.0
 *   1,aa,dolphin,88.0
 *   2,bb,spark,90.0
 *   2,bb,redis,86.0
 *   .....
 *
 **/
public class _01_Xecersize_2 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("ex2");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("./spark_data/excersize_2/input/some.data");

        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                // {"id":1,"name":"aa",courses:[{"name":"dolphin","score":88.0},{"name":"java","score":80.0}, {"name":"python","score":99.0}]}
                JSONObject jsonObject = JSON.parseObject(s);
                int id = jsonObject.getIntValue("id");
                String name = jsonObject.getString("name");
                JSONArray array = jsonObject.getJSONArray("courses");


                ArrayList<CourseInfo> lst = new ArrayList<>();
                for (int i = 0; i < array.size(); i++) {
                    JSONObject obj = array.getJSONObject(i);
                    String courseName = obj.getString("name");
                    double score = obj.getDoubleValue("score");
                    CourseInfo courseInfo = new CourseInfo(courseName, score);
                    // 把提取的课程信息对象，放入lst先存着
                    lst.add(courseInfo);
                }

                // 对lst中的课程信息对象按照分数高低排序
                Collections.sort(lst);


                // 拼接最终需要的结果
                ArrayList<String> resLst = new ArrayList<>();
                for (int i = 0; i < Math.min(2, lst.size()); i++) {
                    String line = id + "," + name + "," + lst.get(i).getCourseName() + "," + lst.get(i).getScore();
                    resLst.add(line);
                }

                return resLst.iterator();
            }
        });


        rdd2.saveAsTextFile("./spark_data/excersize_2/output");


    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CourseInfo implements Comparable<CourseInfo>{
        private String courseName;
        private double score;

        @Override
        public int compareTo(CourseInfo other) {
            return Double.compare(other.score, this.score);
        }
    }


}

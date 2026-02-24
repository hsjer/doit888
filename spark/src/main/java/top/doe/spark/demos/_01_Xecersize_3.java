package top.doe.spark.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/10
 * @Desc: 学大数据，上多易教育
 * 对练习2的数据，
 * {"id":1,"name":"aa",courses:[{"name":"dolphin","score":88.0},{"name":"java","score":80.0}, {"name":"python","score":99.0}]}
 * {"id":2,"name":"bb",courses:[{"name":"redis","score":86.0},{"name":"spark","score":90.0},{"name":"flink","score":80.0}]}
 * {"id":3,"name":"cc",courses:[{"name":"hbase","score":88.0},{"name":"hive","score":80.0}, {"name":"kafka","score":92.0}]}
 * {"id":4,"name":"dd",courses:[{"name":"doris","score":88.0},{"name":"java","score":82.0}, {"name":"python","score":85.0}]}
 * {"id":5,"name":"ee",courses:[{"name":"dolphin","score":88.0},{"name":"hive","score":81.0}, {"name":"flume","score":98.0}]}
 * <p>
 * 求每个学生的，考试成绩门数，最高分，最低分，平均分
 * 1,aa,3,99.0,80.0,87.6
 **/
public class _01_Xecersize_3 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("ex3");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("./spark_data/excersize_2/input/some.data");


        JavaRDD<String> rdd2 = rdd1.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                JSONObject jsonObject = JSON.parseObject(v1);
                int id = jsonObject.getIntValue("id");
                String name = jsonObject.getString("name");
                JSONArray array = jsonObject.getJSONArray("courses");


                double sum = 0;
                double max = 0;
                double min = 0;
                int count = 0;
                for (int i = 0; i < array.size(); i++) {
                    JSONObject obj = array.getJSONObject(i);
                    String courseName = obj.getString("name");
                    double score = obj.getDoubleValue("score");

                    count++;
                    sum += score;
                    max = score > max ? score : max;
                    min = score < min ? score : min;

                }


                return id + "," + name + "," + count + "," + max + "," + min + "," + sum / count;
            }
        });

        rdd2.saveAsTextFile("./spark_data/excersize_2/output");


    }

}

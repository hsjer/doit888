package top.doe.spark.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/10
 * @Desc: 学大数据，上多易教育
 *
 *  输入数据：
 *  {"id":1,"name":"zs","courses":["a","b","e"]}
 *  {"id":2,"name":"ls","courses":["a","b","c","d"]}
 *  {"id":3,"name":"ww","courses":["a","b","c","e","f"]}
 *  {"id":4,"name":"zl","courses":["a","f","b"]}
 *  {"id":5,"name":"tq","courses":["a","d","c"]}
 *
 *  加工成：
 *   1,zs,a
 *   1,zs,b
 *   1,zs,e
 *   2,ls,a
 *   2,ls,b
 *   2,ls,c
 *   .....
 *
 **/
public class _01_Xecersize_1 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("xer1");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("./spark_data/excersize_1/input");

        JavaRDD<String> rdd2 = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {

                // {"id":1,"name":"zs","courses":["a","b","e"]}
                JSONObject jsonObject = JSON.parseObject(s);
                int id = jsonObject.getIntValue("id");
                String name = jsonObject.getString("name");
                JSONArray array = jsonObject.getJSONArray("courses");

                String[] result = new String[array.size()];

                for (int i = 0; i < array.size(); i++) {
                    String c = array.getString(i);

                    // 拼接 1,zs,a
                    String line = id + "," + name + "," + c;
                    result[i] = line;
                }


                return Arrays.stream(result).iterator();
            }
        });


        rdd2.saveAsTextFile("./spark_data/excersize_1/output");


    }
}

package top.doe.spark.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.io.*;
import java.util.HashMap;
import java.util.List;

public class _22_BroadCastDemo {

    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf();
        conf.setAppName("xx");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 订单信息，映射成RDD
        JavaRDD<String> rdd = sc.textFile("spark_data/excersize_3/input/");

        BufferedReader br = new BufferedReader(new FileReader("spark_data/user_info/user.info"));
        HashMap<Integer, User> userInfoMap = new HashMap<>();
        String line = null;
        while ((line = br.readLine()) != null) {
            String[] split = line.split(",");
            User user = new User(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]));
            userInfoMap.put(user.id,user);
        }

        // 广播 用户信息 的hashmap
        Broadcast<HashMap<Integer, User>> bc = sc.broadcast(userInfoMap);


        //
        JavaRDD<String> rdd1 = sc.textFile("spark_data/user_info/user.info");
        List<String> lst = rdd1.collect();
        sc.broadcast(lst);


        userInfoMap.put(3,new User(3,"zl",28));


        // 做map端join
        JavaRDD<String> joined = rdd.map(new Function<String, String>() {
            @Override
            public String call(String orderData) throws Exception {

                // 取到executor中的广播数据
                HashMap<Integer, User> userInfo = bc.value();

                // 解析订单
                // {"uid":1,"oid":"o_1","pid":1,"amt":78.8}
                JSONObject jsonObject = JSON.parseObject(orderData);
                int uid = jsonObject.getIntValue("uid");
                // 取用户信息
                User user = userInfo.get(uid);

                // 填充数据
                jsonObject.put("user_name", user.name);
                jsonObject.put("user_age", user.age);

                return jsonObject.toJSONString();
            }
        });


        joined.foreach(s-> System.out.println(s));

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class User implements Serializable {
        private int id;
        private String name;
        private int age;

    }

}

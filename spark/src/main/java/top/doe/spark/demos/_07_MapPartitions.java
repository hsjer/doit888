package top.doe.spark.demos;


import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/11
 * @Desc: 学大数据，上多易教育
 *   输入数据：
 *    {"uid":1,"oid":"o_1","pid":1,"amt":78.8}
 *    {"uid":1,"oid":"o_2","pid":2,"amt":68.8}
 *    {"uid":1,"oid":"o_3","pid":1,"amt":160.0}
 *
 *   需求：
 *     根据数据中的pid，去mysql数据库中查询这个商品的名称，补全数据
 **/
public class _07_MapPartitions {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.set("spark.default.parallelism","2");
        conf.setAppName("name");


        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("./spark_data/excersize_3/input/order.data",2);
        System.out.println(rdd1.getNumPartitions() + "----------------------------------");

        // 在map的函数的构造方法中去创建数据库连接上行不通的
        // 因为函数的构造是发生在job的客户端； 运行时的分布式task拿到的是函数的序列化对象；
        // 而数据库连接是不适合在一个机器上创建后序列化发送到别的机器上去使用
        // JavaRDD<String> res = rdd1.map(new QueryMapper());

        JavaRDD<String> resRDD = rdd1.mapPartitions(new QueryFlatmapFunction());
        resRDD.foreach(s-> System.out.println(s));


    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order implements Serializable {
        private int uid;
        private String oid;
        private int pid;
        private double amt;
        private String pname;
    }


    public static class QueryFlatmapFunction implements FlatMapFunction<Iterator<String>,String> {

        @Override
        public Iterator<String> call(Iterator<String> partitionIterator) throws Exception {

            // 创建后续处理数据时需要使用的公共工具
            // 在本需求中就是创建jdbc连接
            Connection connection = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/doit50", "root", "ABC123.abc123");
            PreparedStatement stmt = connection.prepareStatement("select pname from product where pid= ? ");
            System.out.println("创建了数据库连接------------------------------------------------");

            // 公共工具创建完之后，再利用分区迭代器去逐行映射数据
            ArrayList<String> list = new ArrayList<>();
            while(partitionIterator.hasNext()){
                String json = partitionIterator.next();
                Order order = JSON.parseObject(json,Order.class);
                int pid = order.getPid();
                // 查数据库
                stmt.setInt(1,pid);
                ResultSet resultSet = stmt.executeQuery();
                resultSet.next();
                String pname = resultSet.getString("pname");
                order.setPname(pname);

                list.add(JSON.toJSONString(order));
            }

            // 最后返回映射完的结果
            return list.iterator();
        }
    }




    public static class QueryMapper implements Function<String,String>{

        public QueryMapper(){
            System.out.println("创建连接");
        }


        @Override
        public String call(String v1) throws Exception {
            System.out.println("来了一条数据");
            return v1;
        }
    }

}

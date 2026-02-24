package top.doe.spark_sql;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class _01_RumenDemo {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", KryoSerializer.class.getName());

        // 创建sparkSession
        SparkSession session =
                SparkSession.builder()
                .appName("demo1")
                .master("local")
                .config("spark.default.parallelism","10")
                .config("spark.sql.shuffle.partitions","5") // 默认200
                //.config(conf)
                .getOrCreate();

        // 把数据源映射成dataset
        Dataset<Row> ds = session.read().json("spark_data/excersize_3/input");

        // 用调方法的形式，写sql ==> 这种sql关键的api叫DSL查询语法
//        Dataset<Row> resDs =
//                ds.where("uid > 2")
//                   .select("uid", "pid", "amt");

        // 更常用的形式，就是写真正的sql

        // 先把dataset注册到sqlSession中，用有一个视图（表）名
        ds.createOrReplaceTempView("t_order");
        // 然后在sqlSession中执行sql
        //Dataset<Row> res = session.sql("select uid,pid,amt from t_order where uid>2");
        //Dataset<Row> res = session.sql("select * from t_order");
        // 统计每个用户的订单总数，消费总额
        Dataset<Row> res = session.sql(
                " select                        " +
                "     uid,                      " +
                "     count(1) as order_cnt,    " +
                "     round(sum(amt),2) as amt  " +
                " from t_order                  " +
                " group by uid                  "
        );

        res.show(10);

        // TODO 求每个用户金额最高的前两个订单



        Thread.sleep(Long.MAX_VALUE);
    }


}

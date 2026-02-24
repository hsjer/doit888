package top.doe.spark_sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.functions;

public class _05_UDAF_Aggregator {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("niube")
                .getOrCreate();

        Dataset<Row> ds3 = spark.read().json("sql_data/datasource/order.data");
        ds3.createOrReplaceTempView("t_order");

        // 注册函数（强类型的函数）
        // 注册略麻烦，需要先用 org.apache.spark.sql.functions.udaf() 把它转成userDefinedFunction类型
        spark.udf().register("my_avg",functions.udaf(new MyAvg(),Encoders.DOUBLE()));

        spark.sql("select uid,my_avg(amt) as amt from t_order group by uid").show();



    }


    /**
     * ---------------------------------------------------------------------------
     */


    // 强类型的
    public static class MyAvg extends Aggregator<Double, Agg, Double> {

        // 初始化累加器
        @Override
        public Agg zero() {
            return new Agg();
        }


        // 聚合数据的逻辑
        @Override
        public Agg reduce(Agg agg, Double a) {
            agg.sum += a;
            agg.count++;

            return agg;
        }


        // 聚合两个累加器
        @Override
        public Agg merge(Agg b1, Agg b2) {

            b1.count += b2.count;
            b1.sum += b2.sum;

            return b1;
        }


        // 返回最终结果
        @Override
        public Double finish(Agg agg) {
            return Math.round(10 * agg.sum / agg.count) / 10.0;
        }


        // buffer类型对应的编码器
        @Override
        public Encoder<Agg> bufferEncoder() {
            return Encoders.bean(Agg.class);
        }


        // 最终输出结果类型的编码器
        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }


    // 必须完全符合javaBean的规范 （有参，无参，getter，setter）
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Agg {
        private int count;
        private double sum;
    }
}

package top.doe.spark_sql;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/25
 * @Desc: 学大数据，上多易教育
 *  RDD 和 sql 混编
 **/
public class _03_RDD_SQL_Hybrid {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("hybrid")
                .config("spark.sql.shuffle.partitions", 10)
                .config("spark.serializer", KryoSerializer.class.getName())
                //.enableHiveSupport()
                .getOrCreate();

        // rdd转dataset
        SparkContext sc = spark.sparkContext();
        // 把scala的sc转成java的sc
        JavaSparkContext jsc = new JavaSparkContext(sc);
        // [2024-09-25,16:30:30],江西,{"uid":1,"oid":"o_1","pid":1,"amt":78.8}
        // [2024-09-25,16:30:30],黑龙江,{"uid":1,"oid":"o_1","pid":1,"amt":78.8}
        JavaRDD<String> rdd = jsc.textFile("sql_data/hybrid");
        // 解析数据
        JavaRDD<OrderBean> beanRdd = rdd.map(line -> {

            String regex = "\\[(.*?)\\],(.*?),(\\{.*?\\})";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(line);
            if(matcher.find()){
                String part1 = matcher.group(1);
                String order_time = part1.replace(",", " ");

                String province = matcher.group(2);

                String json = matcher.group(3);

                OrderBean orderBean = JSON.parseObject(json, OrderBean.class);

                orderBean.setOrder_time(order_time);
                orderBean.setProvince(province);
                return orderBean;
            }

            return null;
        }).filter(Objects::nonNull);



        // 方式1，把rdd转成 dataframe:dataset[row] --------------------------------------------------
        // dataset<Row> 别名叫  dataframe
        Dataset<Row> ds = spark.createDataFrame(beanRdd, OrderBean.class);


        // rdd中的javaBean，会在创建 DataFrame的过程中反射出 表结构定义
        ds.printSchema();
        ds.createOrReplaceTempView("ds_row");
        ds.explain("codegen");
        //spark.sql("select * from ds").show();

        Dataset<Row> res1 = spark.sql("select province,count(distinct uid) as u, cast( sum(amt) as decimal(10,2)) as amt from ds_row group by province");
        res1.show();


        // 方式2，把rdd转成 dataset[T] --------------------------------------------------
        Dataset<OrderBean> dataset = spark.createDataset(beanRdd.rdd(), Encoders.bean(OrderBean.class));

        // dataset[T] ，方便直接调用rdd的算子
        dataset.createOrReplaceTempView("ds_t");
        Dataset<Row> res2 = spark.sql("select province,count(distinct uid) as u, cast( sum(amt) as decimal(10,2)) as amt from ds_t group by province");
        res2.show();

        spark.close();


    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderBean /*implements Serializable*/ {
        private String order_time;
        private String province;
        private int uid;
        private String oid;
        private int pid;
        private BigDecimal amt;
    }

}

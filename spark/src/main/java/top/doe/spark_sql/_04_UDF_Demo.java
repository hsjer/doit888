package top.doe.spark_sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/25
 * @Desc: 学大数据，上多易教育
 * 自定义标量函数的示意:
 * 求多个字段中 最大值/最小值
 **/
public class _04_UDF_Demo {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("niube")
                .getOrCreate();

        Dataset<Row> ds = spark.createDataFrame(Arrays.asList(
                new Info(1, 40, 20, 80, 50),
                new Info(2, 25, 20, 80, 100),
                new Info(3, 50, 40, 80, 50),
                new Info(4, 30, 20, 90, 50)
        ), Info.class);

        ds.createOrReplaceTempView("score");


        // 需要一个接收4参数的标量函数，就实现 UDF4接口
        UDF4<Double, Double, Double, Double, Double> udf4 =
                new UDF4<Double, Double, Double, Double, Double>() {

                    @Override
                    public Double call(Double d1, Double d2, Double d3, Double d4) throws Exception {

                        List<Double> lst = Arrays.asList(d1, d2, d3, d4);
                        Collections.sort(lst);

                        Double min = lst.get(0);
                        Double max = lst.get(lst.size() - 1);

                        return Math.round((max/min)*10)/10.0;
                    }
                };


        // 把函数对象注册到session中
        spark.udf().register("max_min_ratio",udf4, DataTypes.DoubleType);
        spark.sql("select id,max_min_ratio(lan_score,math_score,chm_score,phy_score) from score").show();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Info {
        private int id;
        private double lan_score;
        private double math_score;
        private double chm_score;
        private double phy_score;
    }

}

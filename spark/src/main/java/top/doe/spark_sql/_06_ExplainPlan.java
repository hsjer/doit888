package top.doe.spark_sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/25
 * @Desc: 学大数据，上多易教育
 *   执行计划
 **/
public class _06_ExplainPlan {
    public static void main(String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("niube")
                .getOrCreate();

        //-------------数据准备----------------------------------------------------
        Dataset<Row> ds1 = spark.read().json("sql_data/datasource/order.data");
        //ds1.show();
        ds1.createOrReplaceTempView("t_order");

        // 简单计划打印
        //explain_1(spark);


        Dataset<Row> ds2 = spark.read()
                .option("header","true")
                .option("inferSchema","true")
                .csv("sql_data/datasource/user.csv");
        ds2.show();
        ds2.createOrReplaceTempView("t_user");


        // 复杂计划打印
        spark.sql("SELECT gender \n" +
                "        ,count(1) as order_cnt\n" +
                "        ,sum(amt) as amt_sum\n" +
                "FROM (\n" +
                "    SELECT o.*,u.*\n" +
                "    from t_order o join t_user u on o.uid = u.id\n" +
                "    where amt>100\n" +
                ") tmp\n" +
                "GROUP BY gender ")/*.explain("codegen")*/;

        // 优化测试
        spark.sql("SELECT\n" +
                "uid,\n" +
                "oid,\n" +
                "pid,\n" +
                "amt,\n" +
                "row_number() over(partition by uid order by oid) as rn ,\n" +
                "sum(amt) over(partition by uid order by amt desc)  as accu\n" +
                "from t_order").explain("codegen");

        // 优化测试
        spark.sql("SELECT\n" +
                "uid,\n" +
                "oid,\n" +
                "pid,\n" +
                "amt,\n" +
                "row_number() over(partition by uid order by oid) as rn ,\n" +
                "sum(amt) over(partition by pid order by amt desc)  as accu\n" +
                "from t_order")/*.explain("extended")*/;




    }


    public static void explain_1(SparkSession spark){
        //-------------执行计划查看----------------------------------------------------
        // 打印物理计划
        spark.sql("select * from (select * from t_order) tmp where amt>100").explain("simple");
        System.out.println("----------********---------------------------------");

        // 逻辑计划和物理计划
        spark.sql("select * from (select * from t_order) tmp where amt>100").explain("extended");
        System.out.println("----------********---------------------------------");


        // 物理计划和 动态生成的代码（如果有的话）
        spark.sql("select * from t_order where amt>100").explain("codegen");
        System.out.println("----------********---------------------------------");

        // 格式化打印物理计划
        spark.sql("select * from t_order where amt>100").explain("formatted");
        System.out.println("----------********---------------------------------");

        // 逻辑计划，及其 成本统计信息
        spark.sql("select * from t_order where amt>100").explain("cost");
    }


}

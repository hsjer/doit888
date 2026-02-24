package top.doe.spark_sql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/23
 * @Desc: 学大数据，上多易教育
 *   sparkSql支持的各种数据源演示
 **/
public class _02_DataSources {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("datasource")
                .config(conf)
                .config("spark.sql.shuffle.partitions",2)
                .enableHiveSupport()  // 开启hive支持
                .getOrCreate();

        //-------------CSV------------------------------------------------------
        Dataset<Row> ds = spark.read().format("csv")
                .option("sep", ",")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("quote", "$")   // 什么符号看成引号
                .option("nullValue", "\\N")
                .load("sql_data/datasource/a.csv");


        Dataset<Row> ds2 = spark.read()
                .option("sep", ",")
                .csv("sql_data/datasource/a.csv");


        // 查看表结构
        ds.printSchema();

        ds.show();


        // 查询
        ds.createOrReplaceTempView("tmp");
        Dataset<Row> res = spark.sql("select * from tmp where name is not null");
        //res.write().format("csv").save("");
        //res.write().option("header","true").csv("sql_data/output/");



        //-------------JSON------------------------------------------------------
        Dataset<Row> ds3 = spark.read().format("json")
                //.option("mode", "PERMISSIVE")  // 尽量解析
                .option("mode", "DROPMALFORMED")  // 丢弃不合法的数据
                .option("columnNameOfCorruptRecord", "error")
                .option("prefersDecimal","true")  // 把double值优先解析成decimal
                .load("sql_data/datasource/order.data");
        ds3.show();
        ds3.printSchema();

        // 输出json格式
        ds3.createOrReplaceTempView("ds3");
        Dataset<Row> top2 = spark.sql(
                "with tmp as (\n" +
                "    select\n" +
                "    *,\n" +
                "    row_number() over(partition by uid order by amt desc) as rn\n" +
                "    from ds3\n" +
                ")\n" +
                "select * from tmp where rn<=2");
        top2.show();
        //top2.write().format("json").save("sql_data/json_out");
        /*
        +-----+----+---+---+---+
        |  amt| oid|pid|uid| rn|
        +-----+----+---+---+---+
        |160.0| o_3|  1|  1|  1|
        | 78.8| o_1|  1|  1|  2|
        | 78.8| o_7|  3|  3|  1|
        | 78.8| o_8|  2|  3|  2|
        |120.8| o_4|  2|  2|  1|
        | 78.8|o_10|  2|  4|  1|
        | 78.8| o_9|  2|  4|  2|
        +-----+----+---+---+---+
         */



        //-------------PARQUET------------------------------------------------------
        // 写parquet
        top2.write().format("parquet").save("sql_data/json_out");


        spark.conf().set("spark.sql.parquet.compression.codec","snappy");
        top2.write()
                .option("parquet.compression","true")  // 表示不要压缩(默认是snappy压缩）(如果要压缩，默认是snappy编码）
                .parquet("sql_data/parquet_output/");

        // 读parquet
        Dataset<Row> ds5 = spark.read().parquet("sql_data/parquet_output/");
        ds5.show();
        ds5.createOrReplaceTempView("ds5");
        spark.sql("select * from ds5 where amt>50").show();


        //-------------JDBC------------------------------------------------------
        // 读数据
        Dataset<Row> ds6 = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://doitedu01:3306/doit50")
                //.option("dbtable", "doit50.product")
                .option("user", "root")
                .option("password", "ABC123.abc123")
                .option("query", "select * from product where pid>1")
                .load();
        ds6.show();
        /*
        +---+------+--------+-----+
        |pid| pname|category|brand|
        +---+------+--------+-----+
        |  2|特仑苏|    牛奶| 雀巢|
        |  3|  桃李|    面包| 雀巢|
        +---+------+--------+-----+
         */


        // 写数据
        // top2: 是上面的例子中求出来的：每个用户金额最高的前两个订单
        Properties props = new Properties();
        props.setProperty("user","root");
        props.setProperty("password","ABC123.abc123");

        // 如果目标已存在(如果是jdbc，则是表存在；如果是文件，则是目录已存在)，Append是追加
        //top2.write().mode(SaveMode.Append).jdbc("jdbc:mysql://doitedu01:3306/doit50","od_top2",props);

        // 如果目标已存在，Ignore则忽略本次输出
        top2.write().mode(SaveMode.Ignore).jdbc("jdbc:mysql://doitedu01:3306/doit50","od_top2",props);

        // 如果目标已存在，Overwrite是覆盖
        //top2.write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://doitedu01:3306/doit50","od_top2",props);

        // 如果目标已存在，ErrorIfExists 则报错
        //top2.write().mode(SaveMode.ErrorIfExists).jdbc("jdbc:mysql://doitedu01:3306/doit50","od_top2",props);


        //-------------HIVE表读写-----------------------------------------------------
        Dataset<Row> ds7 = spark.read().table("sale");
        ds7.show();
        /*
        +---+-------+----+---+
        | id|    mth|shop|amt|
        +---+-------+----+---+
        |  1|2024-01|   a|100|
        |  2|2024-01|   a|200|
        |  3|2024-02|   a|100|
        |  4|2024-02|   a|200|
        |  5|2024-02|   a|300|
        |  6|2024-03|   a|100|
        |  7|2024-03|   a|400|
        |  8|2024-01|   b|100|
        |  9|2024-01|   b|300|
        | 10|2024-01|   b|200|
        | 11|2024-02|   b|400|
        | 12|2024-02|   b|200|
        | 13|2024-02|   b|200|
        | 14|2024-03|   b|200|
        | 15|2024-03|   b|500|
        +---+-------+----+---+
         */
        // 随手练习sql
        // 求每个店铺的每个月份的总销售额，及累计到当月的累计总额
        // TODO
        ds7.createOrReplaceTempView("ds7");
        Dataset<Row> accu =
                spark.sql("select\n" +
                        "    shop,\n" +
                        "    mth,\n" +
                        "    round(max(amt)+100),\n" +
                        "    sum(amt) as amt, \n" +
                        "    sum(sum(amt)) over(partition by shop order by mth) as accu_amt\n" +
                        "from ds7\n" +
                        "group by shop,mth");
        accu.explain();




        System.out.println("-----------------------------------------------------------");
        spark.sql("with tmp as (\n" +
                "    select\n" +
                "        shop,\n" +
                "        mth,\n" +
                "        sum(amt) as amt \n" +
                "    from ds7\n" +
                "    group by shop,mth\n" +
                ")\n" +
                "select\n" +
                "    shop,\n" +
                "    mth,\n" +
                "    amt,\n" +
                "    sum(amt) over(partition by shop order by mth) as accu_amt\n" +
                "from tmp\n").explain();

        System.out.println("-----------------------------------------------------------");
        spark.sql(
                "select\n" +
                        "    id,\n" +
                        "    mth,\n" +
                        "    shop,\n" +
                        "    amt,\n" +
                        "    row_number() over(partition by shop order by amt desc) as rn ,\n" +
                        "    sum(amt) over(partition by mth order by id,shop)  as accu\n" +
                        "from ds7"
        ).explain();



    }


}

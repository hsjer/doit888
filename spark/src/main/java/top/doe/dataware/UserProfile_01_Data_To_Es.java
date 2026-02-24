package top.doe.dataware;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;
import scala.Predef$;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

import java.util.HashMap;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/13
 * @Desc: 学大数据，上多易教育
 *   用户画像标签数据导入es
 **/
public class UserProfile_01_Data_To_Es {

    public static void main(String[] args) {


        SparkConf conf = new SparkConf();
        // es客户端相关配置参数
        conf .set("es.index.auto.create", "true")
              .set("es.nodes", "doitedu01")
                .set("es.port", "9200")
                .set("es.nodes.wan.only", "true");

        SparkSession spark = SparkSession.builder()
                .appName("PageViewInterestWordsTopn")
                .config(conf)
                .master("local[1]")
                .enableHiveSupport()
                .getOrCreate();

        // 读取hive中的标签表数据
        Dataset<Row> df = spark.read().table("tmp.user_profile_01").where("dt='2024-04-04'");


        // 配置es写数据的相关参数
        HashMap<String, String> map = new HashMap<>();
        map.put("es.mapping.id","guid");
        map.put("es.write.operation","upsert");

        // 把java的map转成scala的map
        Map<String, String> scalaMap = JavaConverters.mapAsScalaMap(map).toMap(Predef$.MODULE$.conforms());

        // 调用spark-es整合插件工具，将数据写入到es
        EsSparkSQL.saveToEs(df,"doit50_profile",scalaMap);

        spark.close();

    }



}

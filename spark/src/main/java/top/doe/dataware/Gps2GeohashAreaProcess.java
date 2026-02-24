package top.doe.dataware;

import ch.hsr.geohash.GeoHash;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.util.Properties;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/13
 * @Desc: 学大数据，上多易教育
 *   gps地理位置参考数据，加工成geohash数据并写入hive
 **/
public class Gps2GeohashAreaProcess {
    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession.builder()
                .master("local[1]")
                .appName("Geohash Area")
                .enableHiveSupport()
                .getOrCreate();

        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","ABC123.abc123");
        Dataset<Row> gpsCk = spark.read().jdbc("jdbc:mysql://doitedu01:3306/realtime_dw", "doit50_gps_ck", properties);

        gpsCk.createTempView("tmp");

        // 自定义 经纬度坐标转 geohash码的自定义函数
        UDF2<Double, Double, String> gps2GeoUDF = new UDF2<Double, Double, String>() {

            @Override
            public String call(Double lat, Double lng) throws Exception {
                return GeoHash.geoHashStringWithCharacterPrecision(lat,lng,6);
            }
        };

        // 注册函数
        spark.udf().register("gps2geo",gps2GeoUDF, DataTypes.StringType);


        // 数据转换并将结果插入hive的表 : dim.geohash_area
        // 这里要注意去重：因为参考点中，有些非常近的参考点，得到的geohash码是相同的
        spark.sql("insert overwrite table dim.geohash_area\n" +
                "select\n" +
                "  geohash,\n" +
                "  province,\n" +
                "  city,\n" +
                "  region \n" +
                "from (\n" +
                "select \n" +
                "    geohash,\n" +
                "    province,\n" +
                "    city,\n" +
                "    region,\n" +
                "    row_number() over(partition by geohash) as rn \n" +
                "from (\n" +
                "select\n" +
                "    gps2geo(bd09_lat,bd09_lng) as geohash,\n" +
                "    province,\n" +
                "    city,\n" +
                "    region\n" +
                "from tmp where bd09_lat is not null ) o1\n" +
                ") o2\n" +
                "where rn=1");


        spark.close();

    }
}

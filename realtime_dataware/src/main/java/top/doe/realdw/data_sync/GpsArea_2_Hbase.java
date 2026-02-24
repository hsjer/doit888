package top.doe.realdw.data_sync;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.doe.realdw.udfs.Gps2GeoHash;

public class GpsArea_2_Hbase {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        // 参数： execution.checkpointing.timeout = 默认 10min
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // execution.checkpointing.tolerable-failed-checkpoints = 0 未生效
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);


        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 建表，映射工具库中的  gps_area 表
        tenv.executeSql(
                " create table tmp_area_mysql (                                  "+
                        "   lat double,                                          "+
                        "   lng double,                                          "+
                        "   province string,                                     "+
                        "   city string,                                         "+
                        "   region string                                        "+
                        " ) WITH (                                               "+
                        "    'connector' = 'jdbc',                               "+
                        "    'url' = 'jdbc:mysql://doitedu01:3306/realtime_dw',  "+
                        "    'table-name' = 'gps_area',                          "+
                        "    'driver' = 'com.mysql.cj.jdbc.Driver',              "+
                        "    'username' = 'root',                                "+
                        "    'password' = 'ABC123.abc123'                        "+
                        " );                                                     "
        );

        // 建表，映射hbase中的目标表
        // hbase>create 'dim_geo_area','f',SPLITS=>['c','g','k','p']
        tenv.executeSql(
                " create table dim_geo_area_hbase(                             "+
                        "    geohash_md5  string,                              "+
                        "    f row<province string,city string,region string>, "+
                        "    primary key(geohash_md5) not enforced             "+
                        " ) WITH (                                             "+
                        "  'connector' = 'hbase-2.2',                          "+
                        "  'table-name' = 'dim_geo_area',                      "+
                        "  'zookeeper.quorum' = 'doitedu01:2181'               "+
                        " )                                                    "
        );

        // 注册函数
        tenv.createTemporaryFunction("geo", Gps2GeoHash.class);

        // insert
        tenv.executeSql(
                      " insert into dim_geo_area_hbase                         "+
                        " select                                                 "+
                        "     substring(md5(geo(lat,lng)),0,10) as geohash_md5   "+
                        "     ,row(province                                      "+
                        "           ,city                                        "+
                        "           ,region) as f                                "+
                        " from tmp_area_mysql                                    "+
                        " where lat is not null                                  "+
                        "     and lng is not null                                "+
                        "     and province is not null                           "+
                        "     and city is not null                               "+
                        "     and region is not null                             "
        );


    }
}

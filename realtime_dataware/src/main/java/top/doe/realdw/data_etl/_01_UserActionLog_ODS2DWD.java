package top.doe.realdw.data_etl;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.doe.realdw.udfs.Gps2GeoHash;

public class _01_UserActionLog_ODS2DWD {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        // 参数： execution.checkpointing.timeout = 默认 10min
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // execution.checkpointing.tolerable-failed-checkpoints = 0 未生效
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.setParallelism(1);


        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 建表，映射 kafka中的ods层的 用户行为日志topic [kafka中要提前存在该topic]
        tenv.executeSql(
                " create table ods_events_kafka (                              "+
                        "   release_channel string,                            "+
                        "   device_type string,                                "+
                        "   session_id string,                                 "+
                        "   lat double,                                        "+
                        "   lng double,                                        "+
                        "   user_name string,                                  "+
                        "   event_id string,                                   "+
                        "   action_time bigint,                                "+
                        "   properties map<string,string>,                     "+
                        // 处理时间语义的时间
                        "   pt as proctime()                                   "+
                        " ) WITH (                                             "+
                        "   'connector' = 'kafka',                             "+
                        "   'topic' = 'ods-events',                            "+
                        "   'properties.bootstrap.servers' = 'doitedu01:9092', "+
                        "   'properties.group.id' = 'doit50_g1',               "+
                        "   'scan.startup.mode' = 'latest-offset',             "+
                        "   'value.format' = 'json',                           "+
                        "   'value.fields-include' = 'EXCEPT_KEY'              "+
                        " )                                                    "
        );

        // 建表，映射hbase中的维表：用户注册信息表  [hbase中要存在 dim_user_info]
        tenv.executeSql(
                " create table dim_user_info_hbase(                     "+
                        "      username string                          "+
                        "      ,f  ROW<user_id bigint                   "+
                        "            ,member_level_id int              "+
                        "            ,password string                  "+
                        "            ,nickname string                  "+
                        "            ,phone string                     "+
                        "            ,status int                       "+
                        "            ,create_time timestamp(3)         "+
                        "            ,icon string                      "+
                        "            ,gender int                       "+
                        "            ,birthday date                    "+
                        "            ,city string                      "+
                        "            ,job string                       "+
                        "            ,personalized_signature string    "+
                        "            ,source_type int                  "+
                        "            ,integration int                  "+
                        "            ,growth int                       "+
                        "            ,luckey_count int                 "+
                        "            ,history_integration int          "+
                        "            ,modify_time timestamp(3)>        "+
                        "    ,primary key(username) not enforced       "+
                        " ) WITH (                                     "+
                        "  'connector' = 'hbase-2.2',                  "+
                        "  'table-name' = 'dim_user_info',             "+
                        "  'zookeeper.quorum' = 'doitedu01:2181'       "+
                        " )                                            "
        );

        // 建表，映射hbase中的维表：地理位置信息表 [hbase中要存在 dim_area_info]
        tenv.executeSql(
                " create table dim_geo_area_hbase(                             "+
                        "    geohash_md5  string,                              "+
                        "    f row<province string,city string,region string>, "+
                        "    primary key(geohash_md5) not enforced             "+
                        " ) WITH (                                             "+
                        "  'connector' = 'hbase-2.2'                           "+
                        "  ,'table-name' = 'dim_geo_area'                       "+
                        "  ,'zookeeper.quorum' = 'doitedu01:2181'               "+
                        // 开启lookup缓存功能
                        "  ,'lookup.cache' = 'PARTIAL'                          "+
                        "  ,'lookup.partial-cache.max-rows' = '50000'           "+
                        "  ,'lookup.partial-cache.expire-after-access' = '10 min'        "+
                        "  ,'lookup.partial-cache.caching-missing-key' = 'true'          "+
                        " )                                                               "
        );

        // 建表，映射hbase中的维表：页面信息表  [hbase中要存在 dim_page_info]
        tenv.executeSql(
                " create table dim_page_info_hbase(               "+
                "     url_prefix  string,                         "+
                "     f row<page_type string,                     "+
                "           page_service string,                  "+
                "           page_channel string,                  "+
                "           page_lanmu string>,                   "+
                "     primary key(url_prefix) not enforced        "+
                " ) with (                                        "+
                "      'connector' = 'hbase-2.2',                 "+
                "      'table-name' = 'dim_page_info',            "+
                "      'zookeeper.quorum' = 'doitedu01:2181'      "+
                // 开启lookup缓存功能
                "  ,'lookup.cache' = 'PARTIAL'                          "+
                "  ,'lookup.partial-cache.max-rows' = '50000'           "+
                "  ,'lookup.partial-cache.expire-after-access' = '10 min'        "+
                "  ,'lookup.partial-cache.caching-missing-key' = 'true'          "+
                " )                                                               "
        );


        // 建表，映射 kafka中的 dwd层的  用户行为明细 topic  [kafka中要提前存在该topic]
        tenv.executeSql("create table dwd_events_kafka(      \n" +
                // 原始用户行为日志中的字段
                "     release_channel    string              \n" +
                "    ,device_type        string              \n" +
                "    ,session_id         string              \n" +
                "    ,lat                double              \n" +
                "    ,lng                double              \n" +
                "    ,user_name          string              \n" +
                "    ,event_id           string              \n" +
                "    ,action_time        bigint              \n" +
                "    ,properties         map<string,string>  \n" +

                // 用户注册信息表中的各维度字段
                "    ,user_id bigint\n" +
                "    ,member_level_id bigint\n" +
                "    ,nickname string\n" +
                "    ,phone string\n" +
                "    ,status int\n" +
                "    ,create_time timestamp(3)\n" +
                "    ,icon string\n" +
                "    ,gender int\n" +
                "    ,birthday date\n" +
                "    ,register_city string\n" +
                "    ,job string\n" +
                "    ,personalized_signature string\n" +
                "    ,source_type int\n" +
                "    ,integration int\n" +
                "    ,growth int\n" +
                "    ,luckey_count int\n" +
                "    ,history_integration int\n" +
                "    ,modify_time timestamp(3)\n" +

                // gps地理位置信息表中的维度字段
                "    ,province   string      \n" +
                "    ,city       string      \n" +
                "    ,region     string      \n" +

                // 页面信息表中的维度字段
                "    ,page_type       string \n" +
                "    ,page_service    string \n" +
                "    ,page_channel    string \n" +
                "    ,page_lanmu      string \n" +

                ") WITH (                   \n" +
                "    'connector' = 'kafka', \n" +
                "    'topic' = 'dwd-events',\n" +
                "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                "    'properties.group.id' = 'doit50_g1',  \n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json',              \n" +
                "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                ")                                                    ");



        // 建表，映射 doris中的 dwd层的  用户行为明细表 [doris中要提前建表]
        tenv.executeSql(
                " create table user_events_detail_doris (                            "+
                        "     event_id                   varchar(100)               "+

                        "     ,province                  varchar(100)               "+
                        "     ,city                      varchar(100)               "+
                        "     ,region                    varchar(100)               "+

                        "     ,dt                        date                       "+  // 分区用
                        "     ,release_channel           varchar(100)               "+
                        "     ,device_type               varchar(100)               "+
                        "     ,session_id                varchar(100)               "+
                        "     ,lat                       double                     "+
                        "     ,lng                       double                     "+

                        "     ,user_name                 varchar(100)               "+
                        "     ,action_time               bigint                     "+
                        "     ,properties                map<string,string>         "+
                        "     ,user_id                   bigint                     "+
                        "     ,member_level_id           int                        "+
                        "     ,nickname                  varchar(100)               "+
                        "     ,phone                     varchar(100)               "+
                        "     ,status                    int                        "+
                        "     ,create_time               timestamp(3)               "+
                        "     ,icon                      varchar(100)               "+
                        "     ,gender                    int                        "+
                        "     ,birthday                  date                       "+
                        "     ,register_city             varchar(100)               "+
                        "     ,job                       varchar(100)               "+
                        "     ,personalized_signature    varchar(100)               "+
                        "     ,source_type               int                        "+
                        "     ,integration               int                        "+
                        "     ,growth                    int                        "+
                        "     ,luckey_count              int                        "+
                        "     ,history_integration       int                        "+
                        "     ,modify_time               timestamp(3)               "+

                        "     ,page_type                 varchar(100)               "+
                        "     ,page_service              varchar(100)               "+
                        "     ,page_channel              varchar(100)               "+
                        "     ,page_lanmu                varchar(100)               "+
                        " ) WITH (                                                  "+
                        "    'connector' = 'doris',                                 "+
                        "    'fenodes' = 'doitedu01:8030',                            "+
                        "    'table.identifier' = 'dwd.user_events_detail',         "+
                        "    'username' = 'root',                                   "+
                        "    'password' = '123456',                                   "+
                        "    'sink.label-prefix' = 'doris_label-002'                "+
                        " )                                                         "

        );


        // 维度关联
        tenv.createTemporaryFunction("geo", Gps2GeoHash.class);
        tenv.executeSql(
                        " CREATE TEMPORARY VIEW joined AS                                             "+
                        " SELECT  /*+ LOOKUP('table'='dim_user_info_hbase', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='100ms','max-attempts'='3') */   "+
                        "                                                                             "+
                        "  a.release_channel                                                          "+
                        " ,a.device_type                                                              "+
                        " ,a.session_id                                                               "+
                        " ,a.lat                                                                      "+
                        " ,a.lng                                                                      "+
                        " ,a.user_name                                                                "+
                        " ,a.event_id                                                                 "+
                        " ,a.action_time                                                              "+
                        " ,a.properties                                                               "+
                        "                                                                             "+
                        " ,u.f.user_id                                                                "+
                        " ,u.f.member_level_id                                                        "+
                        " ,u.f.nickname                                                               "+
                        " ,u.f.phone                                                                  "+
                        " ,u.f.status                                                                 "+
                        " ,u.f.create_time                                                            "+
                        " ,u.f.icon                                                                   "+
                        " ,u.f.gender                                                                 "+
                        " ,u.f.birthday                                                               "+
                        " ,u.f.city as register_city                                                  "+
                        " ,u.f.job                                                                    "+
                        " ,u.f.personalized_signature                                                 "+
                        " ,u.f.source_type                                                            "+
                        " ,u.f.integration                                                            "+
                        " ,u.f.growth                                                                 "+
                        " ,u.f.luckey_count                                                           "+
                        " ,u.f.history_integration                                                    "+
                        " ,u.f.modify_time                                                            "+
                        "                                                                             "+
                        " ,g.f.province                                                               "+
                        " ,g.f.city                                                                   "+
                        " ,g.f.region                                                                 "+
                        "                                                                             "+
                        " ,p.f.page_type                                                              "+
                        " ,p.f.page_service                                                           "+
                        " ,p.f.page_channel                                                           "+
                        " ,p.f.page_lanmu                                                             "+
                        "                                                                             "+
                        "                                                                             "+
                        " FROM ods_events_kafka a                                                     "+
                        " LEFT JOIN dim_user_info_hbase FOR SYSTEM_TIME AS OF a.pt   as  u            "+
                        " ON a.user_name = u.username                                                 "+
                        "                                                                             "+
                        " LEFT JOIN dim_geo_area_hbase FOR SYSTEM_TIME AS OF a.pt as g                "+
                        " ON   substring(md5(geo(a.lat,a.lng)),0,10)   = g.geohash_md5                "+
                        "                                                                             "+
                        " LEFT JOIN dim_page_info_hbase  FOR SYSTEM_TIME AS OF a.pt as p              "+
                        " ON regexp_extract(a.properties['url'],'(^/.*/).*?',1)=p.url_prefix          "
        );



        // 关联结果插入  kafka
        tenv.executeSql("insert into dwd_events_kafka select * from joined");


        // 关联结果插入 doris
        tenv.executeSql(
                        " insert into user_events_detail_doris          "+
                        " select                                        "+
                        "     event_id                                  "+
                        "                                               "+
                        "     ,province                                 "+
                        "     ,city                                     "+
                        "     ,region                                   "+
                        "                                               "+
                        "     ,to_date(date_format(to_timestamp_ltz(action_time,3),'yyyy-MM-dd'))  as dt               "+
                        "     ,release_channel                          "+
                        "     ,device_type                              "+
                        "     ,session_id                               "+
                        "     ,lat                                      "+
                        "     ,lng                                      "+
                        "                                               "+
                        "     ,user_name                                "+
                        "     ,action_time                              "+
                        "     ,properties                               "+
                        "     ,user_id                                  "+
                        "     ,member_level_id                          "+
                        "     ,nickname                                 "+
                        "     ,phone                                    "+
                        "     ,status                                   "+
                        "     ,create_time                              "+
                        "     ,icon                                     "+
                        "     ,gender                                   "+
                        "     ,birthday                                 "+
                        "     ,register_city                            "+
                        "     ,job                                      "+
                        "     ,personalized_signature                   "+
                        "     ,source_type                              "+
                        "     ,integration                              "+
                        "     ,growth                                   "+
                        "     ,luckey_count                             "+
                        "     ,history_integration                      "+
                        "     ,modify_time                              "+
                        "                                               "+
                        "     ,page_type                                "+
                        "     ,page_service                             "+
                        "     ,page_channel                             "+
                        "     ,page_lanmu                               "+
                        " from joined                                   "
        );


    }
}

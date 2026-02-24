package top.doe.realdw.data_etl;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.doe.realdw.udfs.TimeTruncUDF;
import top.doe.realdw.utils.EnvUtil;

public class _02_TrafficOlapAggregate_01 {

    public static void main(String[] args) {

        StreamTableEnvironment tenv = EnvUtil.getTableEnv();


        // 建表, 映射 行为日志明细   kafka(dwd)
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

                "    ,rt as to_timestamp_ltz(action_time,3) " +
                "    ,watermark for rt as rt   " +

                ") WITH (                   \n" +
                "    'connector' = 'kafka', \n" +
                "    'topic' = 'dwd-events',\n" +
                "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                "    'properties.group.id' = 'doit50_g1',  \n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json',              \n" +
                "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                ")                                                    ");


        // 建表，映射doris中的聚合表
        tenv.executeSql(
                        " create table sink_doris (                                  "+
                        "      dt               date                                 "+
                        "     ,time_60m         string                               "+
                        "     ,time_30m         string                               "+
                        "     ,time_10m         string                               "+
                        "     ,time_05m         string                               "+
                        "     ,user_id          bigint                               "+
                        "     ,register_dt      date                                 "+
                        "     ,session_id       string                               "+
                        "     ,release_channel  string                               "+
                        "     ,device_type      string                               "+
                        "     ,gps_province     string                               "+
                        "     ,gps_city         string                               "+
                        "     ,gps_region       string                               "+
                        "     ,page_type        string                               "+
                        "     ,page_url         string                               "+  // 维度
                        "     ,pv_amt           bigint                               "+  // 指标
                        " ) with (                                                   "+
                        "     'connector' = 'doris',                                 "+
                        "     'fenodes' = 'doitedu01:8030',                          "+
                        "     'table.identifier' = 'dws.actionlog_traffic_agg_01',   "+
                        "     'username' = 'root',                                   "+
                        "     'password' = '123456',                                 "+
                        "     'sink.label-prefix' = 'doris_label-003'                "+
                        " )                                                          "
        );


        // 计算逻辑sql
        tenv.createTemporaryFunction("time_trunc", TimeTruncUDF.class);
        tenv.executeSql(
                        " INSERT INTO   sink_doris                                                    "+
                        " SELECT                                                                      "+
                        "  to_date(date_format(window_end,'yyyy-MM-dd')) AS dt                        "+
                        " ,time_trunc(action_time,60) as time_60m                                     "+
                        " ,time_trunc(action_time,30) as time_30m                                     "+
                        " ,time_trunc(action_time,10) as time_10m                                     "+
                        " ,time_trunc(action_time,5 ) as time_05m                                     "+
                        " ,user_id                                                                    "+
                        " ,to_date(date_format(create_time,'yyyy-MM-dd')) AS register_dt              "+
                        " ,session_id                                                                 "+
                        " ,release_channel                                                            "+
                        " ,device_type                                                                "+
                        " ,province as gps_province                                                   "+
                        " ,city     as gps_city                                                       "+
                        " ,region   as gps_region                                                     "+
                        " ,page_type                                                                  "+
                        " ,properties['url'] as page_url                                              "+
                        " ,count(event_id) filter(where event_id = 'page_view') as pv_amt             "+
                        " FROM TABLE(                                                                 "+
                        "     TUMBLE(TABLE dwd_events_kafka,DESCRIPTOR(rt),INTERVAL '1' MINUTE )      "+
                        " )                                                                           "+
                        " GROUP BY                                                                    "+
                        " window_start                                                                "+
                        " ,window_end                                                                 "+
                        " ,time_trunc(action_time,60)                                                 "+
                        " ,time_trunc(action_time,30)                                                 "+
                        " ,time_trunc(action_time,10)                                                 "+
                        " ,time_trunc(action_time,5 )                                                 "+
                        " ,user_id                                                                    "+
                        " ,date_format(create_time,'yyyy-MM-dd')                                      "+
                        " ,session_id                                                                 "+
                        " ,release_channel                                                            "+
                        " ,device_type                                                                "+
                        " ,province                                                                   "+
                        " ,city                                                                       "+
                        " ,region                                                                     "+
                        " ,page_type                                                                  "+
                        " ,properties['url']                                                          "
        );



    }



}

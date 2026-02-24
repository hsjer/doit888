package top.doe.realdw.data_etl;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.doe.realdw.utils.EnvUtil;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/26
 * @Desc: 学大数据，上多易教育
 *   视频播放分析主题olap支撑表
 **/
public class _04_VideoPlayAnalyse {
    public static void main(String[] args) {
        StreamTableEnvironment tenv = EnvUtil.getTableEnv();

        // 建表，映射kafka的dwd的行为明细
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




        // 建表，映射doris的聚合表
        tenv.executeSql(
                        " create table doris_sink (                                "+
                        "       dt      date,                                      "+
                        "       province string,                                   "+
                        "       city string,                                       "+
                        "       region string,                                     "+
                        "       device_type string,                                "+
                        "       release_channel string,                            "+
                        "       user_id  bigint,                                   "+
                        "       play_id  string,                                   "+
                        "       video_id bigint,                                   "+
                        "       segment_id  int,                                   "+
                        "       start_time bigint,                                 "+
                        "       end_time bigint                                    "+
                        " ) with (                                                 "+
                        "     'connector' = 'doris',                               "+
                        "     'fenodes' = 'doitedu01:8030',                        "+
                        "     'table.identifier' = 'dws.actionlog_video_play',     "+
                        "     'username' = 'root',                                 "+
                        "     'password' = '123456',                               "+
                        "     'sink.label-prefix' = 'doris_label-004'              "+
                        " )                                                        "
        );

        // sql
        tenv.executeSql(
                        "insert into doris_sink  " +
                        "with tmp as (                                                                          \n "+
                        "     select                                                                             \n "+
                        "       --dt                                                                             \n "+
                        "       province                                                                         \n "+
                        "       ,city                                                                            \n "+
                        "       ,region                                                                          \n "+
                        "       ,device_type                                                                     \n "+
                        "       ,release_channel                                                                 \n "+
                        "       ,user_id                                                                         \n "+
                        "       ,action_time                                                                     \n "+
                        "       ,properties['play_id']   as   play_id                                            \n "+
                        "       ,cast(properties['video_id'] as bigint) as   video_id                                           \n "+
                        "       --segment_id                                                                     \n "+
                        "       --start_time                                                                     \n "+
                        "       --end_time                                                                       \n "+
                        "       ,sum(if(event_id = 'video_resume',1,0))                                          \n "+
                        "            over(partition by user_id,properties['play_id'] order by rt) as segment_id  \n "+
                        "       ,rt                                                                              \n "+
                        "                                                                                        \n "+
                        "     from dwd_events_kafka                                                              \n "+
                        "     where event_id in                                                                  \n "+
                        "     (                                                                                  \n "+
                        "       'video_play',                                                                    \n "+
                        "       'video_hb',                                                                      \n "+
                        "       'video_pause',                                                                   \n "+
                        "       'video_resume',                                                                  \n "+
                        "       'video_stop'                                                                     \n "+
                        "     )                                                                                  \n "+
                        " )                                                                                      \n "+
                        " select                                                                                 \n "+
                        "  to_date(date_format(to_timestamp_ltz(min(action_time),3),'yyyy-MM-dd')) as dt         \n "+
                        " ,province                                                                              \n "+
                        " ,city                                                                                  \n "+
                        " ,region                                                                                \n "+
                        " ,device_type                                                                           \n "+
                        " ,release_channel                                                                       \n "+
                        " ,user_id                                                                               \n "+
                        " ,play_id                                                                               \n "+
                        " ,video_id                                                                              \n "+
                        " ,segment_id                                                                            \n "+
                        " ,min(action_time) as start_time                                                        \n "+
                        " ,max(action_time) as end_time                                                         \n "+
                        "                                                                                        \n "+
                        " from table(                                                                            \n "+
                        "     tumble(table tmp ,descriptor(rt),interval '1' minute)                              \n "+
                        " )                                                                                      \n "+
                        " group by                                                                               \n "+
                        "  window_start                                                                           \n "+
                        " ,window_end                                                                            \n "+
                        " ,province                                                                              \n "+
                        " ,city                                                                                  \n "+
                        " ,region                                                                                \n "+
                        " ,device_type                                                                           \n "+
                        " ,release_channel                                                                       \n "+
                        " ,user_id                                                                               \n "+
                        " ,video_id                                                                              \n "+
                        " ,play_id                                                                               \n "+
                        " ,segment_id                                                                            \n "
        ).print();

    }
}

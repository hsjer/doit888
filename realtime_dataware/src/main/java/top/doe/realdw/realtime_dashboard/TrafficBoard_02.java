package top.doe.realdw.realtime_dashboard;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.doe.realdw.utils.EnvUtil;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/25
 * @Desc: 学大数据，上多易教育
 *   各业务线当天每 5分钟的pv数、uv数、会话数   √     （滚动窗口）
 **/
public class TrafficBoard_02 {

    public static void main(String[] args) {


        StreamTableEnvironment tenv = EnvUtil.getTableEnv();

        // 建表，映射kafka的 dwd层的行为日志数据
        tenv.executeSql("create table dwd_events_kafka(               " +
                "     session_id         string                       " +
                "    ,event_id           string                       " +
                "    ,action_time        bigint                       " +
                "    ,properties         map<string,string>           " +
                "    ,user_id            bigint                       " +
                "    ,create_time        timestamp(3)                 " +
                "    ,page_service       string                       " +
                "    ,rt as to_timestamp_ltz(action_time,3)           " +
                "    ,watermark for rt as rt - interval '0' second    " +
                ") WITH (                                             " +
                "    'connector' = 'kafka',                           " +
                "    'topic' = 'dwd-events',                          " +
                "    'properties.bootstrap.servers' = 'doitedu01:9092', " +
                "    'properties.group.id' = 'doit50_g1',       " +
                "    'scan.startup.mode' = 'latest-offset',     " +
                "    'value.format' = 'json',                   " +
                "    'value.fields-include' = 'EXCEPT_KEY'      " +
                ")                                                    ");

        // 建表，映射目标表（ 工具 mysql）
        tenv.executeSql(
                        " CREATE TABLE mysql_sink (                          "+
                        "   dt           string,                             "+
                        "   update_time  timestamp(3),                       "+
                        "   pv_cnt       bigint,                             "+
                        "   uv_cnt       bigint,                             "+
                        "   ses_cnt      bigint,                             "+
                        "   primary key(dt) not enforced                     "+
                        " )  with (                                          "+
                        "    'connector' = 'jdbc',                           "+
                        "    'url' = 'jdbc:mysql://doitedu01:3306/dw_50',    "+
                        "    'table-name' = 'dashboard_metric_02',           "+
                        "    'driver' = 'com.mysql.cj.jdbc.Driver',          "+
                        "    'username' = 'root',                            "+
                        "    'password' = 'ABC123.abc123'                    "+
                        " );                                                 "
        );

        // 计算逻辑sql
        tenv.executeSql(
                        " insert into mysql_sink                                                 "+
                        " select                                                                 "+
                        "     date_format(window_end,'yyyy-MM-dd') as dt,                        "+
                        "     window_end as update_time,                                         "+
                        "     count(event_id) filter(where event_id = 'page_view') as pv_cnt,    "+
                        "     count(distinct user_id) as uv_cnt,                                 "+
                        "     count(distinct session_id) as ses_cnt                              "+
                        " from table(                                                            "+
                        "     cumulate(table dwd_events_kafka,descriptor(rt),interval '1' second,interval '24' hour)  "+
                        " )                                                                      "+
                        " group by  window_start,window_end                                      "

        );
    }

}

package top.doe.realdw.realtime_dashboard;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.doe.realdw.utils.EnvUtil;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/25
 * @Desc: 学大数据，上多易教育
 *   每 5分钟，每类页面中访问人数最多的前 10个页面及其访问人数  √       (滚动窗口内求分组topn）
 **/
public class TrafficBoard_03 {

    public static void main(String[] args) {


        StreamTableEnvironment tenv = EnvUtil.getTableEnv();

        // 建表，映射kafka的 dwd层的行为日志数据
        tenv.executeSql(
                "create table dwd_events_kafka(               " +
                "     event_id           string                       " +
                "    ,action_time        bigint                       " +
                "    ,properties         map<string,string>           " +
                "    ,user_id            bigint                       " +
                "    ,page_type          string                       " + // 页面类型
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
                        "   page_type         string,                        "+
                        "   `rank`            bigint,                        "+
                        "   page_url          string,                        "+
                        "   uv_cnt            bigint,                        "+
                        "   update_time       timestamp(3),                  "+
                        "   primary key(page_type,`rank`) not enforced       "+
                        " )  with (                                          "+
                        "    'connector' = 'jdbc',                           "+
                        "    'url' = 'jdbc:mysql://doitedu01:3306/dw_50',    "+
                        "    'table-name' = 'dashboard_metric_03',           "+
                        "    'driver' = 'com.mysql.cj.jdbc.Driver',          "+
                        "    'username' = 'root',                            "+
                        "    'password' = 'ABC123.abc123'                    "+
                        " );                                                 "
        );

        // 计算逻辑sql
        tenv.executeSql(
                        " insert into mysql_sink                                                 "+
                        " with tmp as (                                                                                              "+
                        "     select  window_start,window_end,page_type,properties['url'] as page_url,                               "+
                        "             count(distinct user_id) as uv_cnt                                                              "+
                        "     from table(                                                                                            "+
                        "         tumble(table dwd_events_kafka,descriptor(rt),interval '5' minute)                                  "+
                        "     )                                                                                                      "+
                        "     group by window_start,window_end,page_type,properties['url']                                           "+
                        " )                                                                                                          "+
                        "                                                                                                            "+
                        " select  page_type,`rank`,page_url,uv_cnt,window_end as update_time                                         "+
                        " from (                                                                                                     "+
                        " select  window_start,window_end,page_type,page_url,uv_cnt,                                                 "+
                        "         row_number() over(partition by window_start,window_end,page_type order by uv_cnt desc) as `rank`   "+
                        " from tmp ) o                                                                                               "+
                        " where `rank` <=10                                                                                          "

        );
    }

}

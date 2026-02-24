package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/10/24
 * @Desc: 学大数据，上多易教育
{"uid":1,"event_id":"page_load","properties":{"url":"/a","ref":"/x"},"action_time":1704719574000}
{"uid":1,"event_id":"page_load","properties":{"url":"/a","item_id":"p01"},"action_time":1704719575000}
{"uid":1,"event_id":"page_load","properties":{"url":"/a","item_id":"p01"},"action_time":1704719575000}
{"uid":1,"event_id":"page_load","properties":{"url":"/a","item_id":"p01"},"action_time":1704719575000}
{"uid":1,"event_id":"page_load","properties":{"url":"/a","item_id":"p01"},"action_time":1704719575000}
{"uid":3,"event_id":"page_load","properties":{"url":"/a","item_id":"p01"},"action_time":1704719575000}
{"uid":3,"event_id":"page_load","properties":{"url":"/a","item_id":"p01"},"action_time":1704719575000}
{"uid":3,"event_id":"page_load","properties":{"url":"/a","item_id":"p01"},"action_time":1704719575000}
{"uid":3,"event_id":"page_load","properties":{"url":"/a","ref":"/a"},"action_time":1704719576000}
{"uid":4,"event_id":"page_load","properties":{"url":"/a","ref":"/a"},"action_time":1704719576000}
{"uid":4,"event_id":"page_load","properties":{"url":"/a","ref":"/a"},"action_time":1704719576000}
{"uid":4,"event_id":"page_load","properties":{"url":"/a","ref":"/a"},"action_time":1704719576000}
{"uid":2,"event_id":"page_load","properties":{"url":"/a","ref":"/x"},"action_time":1704719577000}
{"uid":2,"event_id":"page_load","properties":{"url":"/a","video_id":"v01"},"action_time":1704719578000}
{"uid":2,"event_id":"page_load","properties":{"url":"/b","ref":"/x"},"action_time":1704719579000}
{"uid":2,"event_id":"page_load","properties":{"url":"/b","ref":"/x"},"action_time":1704719589000}
{"uid":2,"event_id":"page_load","properties":{"url":"/b","ref":"/x"},"action_time":1704719589100}
{"uid":2,"event_id":"page_load","properties":{"url":"/b","ref":"/x"},"action_time":1704719589200}
{"uid":5,"event_id":"page_load","properties":{"url":"/b","ref":"/x"},"action_time":1704719659000}
{"uid":5,"event_id":"page_load","properties":{"url":"/b","ref":"/x"},"action_time":1704719659000}
{"uid":5,"event_id":"page_load","properties":{"url":"/b","ref":"/x"},"action_time":1704719659000}
{"uid":5,"event_id":"page_load","properties":{"url":"/b","ref":"/x"},"action_time":1704719659000}
{"uid":5,"event_id":"page_load","properties":{"url":"/b","ref":"/x"},"action_time":1704719659000}
{"uid":4,"event_id":"page_load","properties":{"url":"/b","ref":"/x"},"action_time":1704719659000}
{"uid":4,"event_id":"page_load","properties":{"url":"/b","ref":"/x"},"action_time":1704719659000}
{"uid":4,"event_id":"page_load","properties":{"url":"/b","ref":"/x"},"action_time":1704719659000}

{"uid":2,"event_id":"page_load","properties":{"url":"/b","ref":"/x"},"action_time":1704719769000}

 **/

public class Demo13_Sql_WindowGroupTopN {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(3);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //tenv.getConfig().set("table.exec.source.idle-timeout","5 s");


        // 建表，映射kafka中的用户行为
        tenv.executeSql(
                        " create table events_kfk(                                   "+
                        "     uid int                                                "+
                        "     ,event_id string                                       "+
                        "     ,properties map<string,string>                         "+
                        "     ,action_time bigint                                    "+
                        "     ,rt as to_timestamp_ltz(action_time,3)                 "+
                        "     ,watermark for rt as rt                                "+
                        " ) with (                                                   "+
                        "     'connector' = 'kafka',                                 "+
                        "     'topic' = 'topic-1',                                   "+
                        "     'properties.bootstrap.servers' = 'doitedu01:9092',     "+
                        "     'properties.group.id' = 'g001',                        "+
                        "     'scan.startup.mode' = 'latest-offset',                 "+
                        "     'value.format' = 'json',                               "+
                        "     'value.fields-include' = 'EXCEPT_KEY'                  "+
                        " )                                                          "
        );


        // 把分组 topn的 changelog结果，写入mysql表
        tenv.executeSql(
                " create table top_n (                                       "+
                        "     window_start timestamp(3),                     "+
                        "     window_end timestamp(3),                       "+
                        "     url string,                                    "+
                        "     rn bigint,                                     "+
                        "     uid int,                                       "+
                        "     pv bigint                                      "+
                        " ) with (                                           "+
                        "     'connector'='jdbc',                            "+
                        "     'url'='jdbc:mysql://doitedu01:3306/doit50',    "+
                        "     'table-name'='window_topn',                    "+
                        "     'username'='root',                             "+
                        "     'password'='ABC123.abc123'                     "+
                        " )                                                  "
        );

//        tenv.executeSql(
//                        " WITH tmp1 AS (                                                                                    "+
//                        "     SELECT uid,properties['url'] as url,rt                                                        "+
//                        "     FROM  events_kfk                                                                              "+
//                        "     WHERE event_id = 'page_load'                                                                  "+
//                        " )                                                                                                 "+
//                       " ,tmp2 AS (                                                                                        "+
//                       "     SELECT  window_start,window_end,url,uid,                                                      "+
//                       "             count(1) as pv                                                                        "+
//                       "     FROM TABLE(                                                                                   "+
//                       "         TUMBLE(TABLE tmp1,descriptor(rt),INTERVAL '5' MINUTE)                                     "+
//                       "     )                                                                                             "+
//                       "     GROUP BY window_start,window_end,url,uid                                                      "+
//                       " )                                                                                                 "+
//                        " select * from tmp2                                                                     "
//        ).print();


        // 查询sql : 实时统计，每个页面访问次数最多的前2名用户
        tenv.executeSql(
                        //" INSERT INTO top_n                                                                                 "+
                        " WITH tmp1 AS (                                                                                    "+
                        "     SELECT uid,properties['url'] as url,rt                                                        "+
                        "     FROM  events_kfk                                                                              "+
                        "     WHERE event_id = 'page_load'                                                                  "+
                        " )                                                                                                 "+
                        " ,tmp2 AS (                                                                                        "+
                        "     SELECT  window_start,window_end,url,uid,                                                      "+
                        "             count(1) as pv                                                                        "+
                        "     FROM TABLE(                                                                                   "+
                        "         TUMBLE(TABLE tmp1,descriptor(rt),INTERVAL '5' MINUTE)                                     "+
                        "     )                                                                                             "+
                        "     GROUP BY window_start,window_end,url,uid                                                      "+
                        " )                                                                                                 "+
                        " ,tmp3 AS (                                                                                        "+
                        "     SELECT  window_start,window_end,url,uid,pv,                                                   "+
                        "             row_number() over(PARTITION BY window_start,window_end,url ORDER BY pv DESC) as rn    "+
                        "     FROM tmp2                                                                                     "+
                        " )                                                                                                 "+
                        " SELECT  window_start,window_end,url,rn,uid,pv                                                     "+
                        " FROM tmp3                                                                                         "+
                        " WHERE rn<=2                                                                                       "
        ).print();

    }
}

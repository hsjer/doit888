package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo12_Sql_GroupTopN {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(4);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().set("table.exec.source.idle-timeout","10 ms");


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
                " create table top_n (                               "+
                        "     url string,                                    "+
                        "     rn bigint,                                     "+
                        "     uid int,                                       "+
                        "     pv bigint,                                     "+
                        "     primary key(url,rn) not enforced               "+
                        "                                                    "+
                        " ) with (                                           "+
                        "     'connector'='jdbc',                            "+
                        "     'url'='jdbc:mysql://doitedu01:3306/doit50',    "+
                        "     'table-name'='top_u',                          "+
                        "     'username'='root',                             "+
                        "     'password'='ABC123.abc123'                     "+
                        " )                                                  "
        );

        // 查询sql : 实时统计，每个页面访问次数最多的前2名用户
        tenv.executeSql( // INSERT 写在最前面
                        " INSERT INTO top_n " +
                        " WITH tmp1 AS (                                                                     "+
                        "     SELECT uid,properties['url'] as url                                            "+
                        "     FROM  events_kfk                                                               "+
                        "     WHERE event_id = 'page_load'                                                   "+
                        " )                                                                                  "+
                        " ,tmp2 AS (                                                                         "+
                        "     SELECT  url,uid,count(1) as pv                                                 "+
                        "     FROM tmp1                                                                      "+
                        "     GROUP BY url,uid                                                               "+
                        " ),tmp3 AS (                                                                        "+
                        "     SELECT  url,uid,pv,row_number() over(PARTITION BY url ORDER BY pv DESC) as rn  "+
                        "     FROM tmp2                                                                      "+
                        " )                                                                                  "+
                        " SELECT url,rn ,uid,pv                                                              "+
                        " FROM tmp3                                                                          "+
                        " WHERE rn<=2                                                                        "
        );

    }
}

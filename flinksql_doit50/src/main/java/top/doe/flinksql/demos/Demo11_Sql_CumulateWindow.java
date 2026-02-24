package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo11_Sql_CumulateWindow {
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

        //tenv.executeSql("select * from events_kfk").print();

        // 查询sql : 每分钟更新一次从当天0点累计到当前的每个页面的pv和uv
        tenv.executeSql(
                        " WITH tmp AS (                                                                     "+
                        "     SELECT uid,properties['url'] as url,rt                                        "+
                        "     FROM  events_kfk                                                              "+
                        "     WHERE event_id = 'page_load'                                                  "+
                        " )                                                                                 "+
                        "                                                                                   "+
                        " SELECT  window_start,window_end,url,count(1) as pv, count(distinct uid) as uv     "+
                        " FROM TABLE(                                                                       "+
                        "     CUMULATE(TABLE tmp, DESCRIPTOR(rt),INTERVAL '1' MINUTE, INTERVAL '24' HOUR)   "+
                        " )                                                                                 "+
                        " GROUP BY window_start,window_end,url                                              "
        ).print();

    }
}

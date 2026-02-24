package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo8_Sql_TumbleWindow {
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

        // 查询sql
        tenv.executeSql(
                        " SELECT window_start,window_end,                                    "+
                        "        sum(if(event_id='page_load',1,0)) as pv,                    "+
                        "        count(distinct uid) as uv                                   "+
                        " FROM TABLE(                                                        "+
                        "     TUMBLE(TABLE events_kfk,DESCRIPTOR(rt),INTERVAL '5' MINUTE)    "+
                        " )                                                                  "+
                        " GROUP BY window_start,window_end                                   "
        ).print();



    }
}

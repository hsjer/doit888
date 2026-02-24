package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo15_Sql_WindowJoin {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().set("table.exec.state.ttl","1 h");


        // 创建源表
        tenv.executeSql("create table t1 (                      \n" +
                "    uid int,                                   \n" +
                "    event_id string,                           \n" +
                "    action_time bigint,                        \n" +
                "    rt as to_timestamp_ltz(action_time,3),     \n" +
                "    watermark for rt as rt                     \n" +
                ") with (                                       \n" +
                "    'connector' = 'kafka',                     \n" +
                "    'topic' = 'ss-1',                          \n" +
                "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                "    'properties.group.id' = 'g001',       \n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json',              \n" +
                "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                ")");

        tenv.executeSql("create table t2 (             \n" +
                "    uid int,                          \n" +
                "    face_status int,                  \n" +
                "    ts   bigint,                      \n" +
                "    rt as to_timestamp_ltz(ts,3),     \n" +
                "    watermark for rt as rt            \n" +
                ") with (                              \n" +
                "    'connector' = 'kafka',            \n" +
                "    'topic' = 'ss-2',                 \n" +
                "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                "    'properties.group.id' = 'g002',                   \n" +
                "    'scan.startup.mode' = 'latest-offset',            \n" +
                "    'value.format' = 'json',                          \n" +
                "    'value.fields-include' = 'EXCEPT_KEY'             \n" +
                ")");


        // window join
        tenv.executeSql(
                " with tmp1 as (                                                "+
                        "     select                                                    "+
                        "         window_start,                                         "+
                        "         window_end,                                           "+
                        "         uid,                                                  "+
                        "         event_id,                                             "+
                        "         action_time                                           "+
                        "     from table(                                               "+
                        "        tumble(table t1,descriptor(rt),interval '1' minute)    "+
                        "     )                                                         "+
                        " )                                                             "+
                        " ,tmp2 as (                                                    "+
                        "     select                                                    "+
                        "         window_start,                                         "+
                        "         window_end,                                           "+
                        "         uid,                                                  "+
                        "         face_status,                                          "+
                        "         ts                                                    "+
                        "     from table(                                               "+
                        "         tumble(table t2,descriptor(rt),interval '1' minute)   "+
                        "     )                                                         "+
                        " )                                                             "+
                        "                                                               "+
                        " select                                                        "+
                        "     tmp1.window_start,                                        "+
                        "     tmp1.window_end,                                          "+
                        "     tmp1.uid,                                                 "+
                        "     tmp1.event_id,                                            "+
                        "     tmp1.action_time,                                         "+
                        "     tmp2.face_status,                                         "+
                        "     tmp2.ts                                                   "+
                        "                                                               "+
                        " from tmp1 join tmp2                                           "+
                        "     on  tmp1.window_start = tmp2.window_start                 "+
                        "     and tmp1.window_end = tmp2.window_end                     "+
                        "     and tmp1.uid = tmp2.uid                                   "
        ).print();

    }
}


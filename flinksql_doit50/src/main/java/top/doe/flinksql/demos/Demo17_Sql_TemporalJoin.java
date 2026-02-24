package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo17_Sql_TemporalJoin {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().set("table.exec.state.ttl","1 h");


        // 创建版本表（识别记录表）: changelog流表
        tenv.executeSql("create table t1 (                      \n" +
                "    uid int primary key not enforced,          \n" +
                "    event_id string,                           \n" +
                "    action_time bigint,                        \n" +
                "    rt as to_timestamp_ltz(action_time,3),     \n" +
                "    watermark for rt as rt                     \n" +
                " ) with (                             "+
                "     'connector' = 'mysql-cdc',       "+
                "     'hostname' = 'doitedu01',        "+
                "     'port' = '3306',                 "+
                "     'username' = 'root',             "+
                "     'password' = 'ABC123.abc123',    "+
                "     'database-name' = 'doit50',      "+
                "     'table-name' = 'face_rec'        "+
                " )                                    "
        );

        // 创建探测表：识别结果表
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
                        " select                        "+
                        "     t2.uid,                   "+
                        "     t2.face_status,           "+
                        "     t2.ts ,                   "+
                        "     t1.action_time            "+
                        " from t2 join t1               "+
                        " for system_time as of t2.rt   "+
                        " on t2.uid = t1.uid            "
        ).print();

    }
}


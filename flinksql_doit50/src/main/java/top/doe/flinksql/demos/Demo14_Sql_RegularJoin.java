package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo14_Sql_RegularJoin {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().set("table.exec.state.ttl","1 h");


        // 创建源表
        tenv.executeSql("create table t1 (\n" +
                "    uid int,                \n" +
                "    event_id string,        \n" +
                "    action_time bigint      \n" +
                ") with (                    \n" +
                "    'connector' = 'kafka',            \n" +
                "    'topic' = 'ss-1',                 \n" +
                "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                "    'properties.group.id' = 'g001',       \n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json',              \n" +
                "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                ")");

        tenv.executeSql("create table t2 (   \n" +
                "    uid int,                \n" +
                "    face_status int,        \n" +
                "    ts   bigint             \n" +
                ") with (                    \n" +
                "    'connector' = 'kafka',            \n" +
                "    'topic' = 'ss-2',                 \n" +
                "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                "    'properties.group.id' = 'g002',       \n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json',              \n" +
                "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                ")");


        // inner join
        tenv.executeSql("select t1.*,t2.* from t1 join t2 on t1.uid = t2.uid ")/*.print()*/;

        // left join
        tenv.executeSql("select t1.*,t2.* from t1 left join t2 on t1.uid = t2.uid ")/*.print()*/;

        // right join
        tenv.executeSql("select t1.*,t2.* from t1 right join t2 on t1.uid = t2.uid ")/*.print()*/;

        // full join
        tenv.executeSql("select t1.*,t2.* from t1 full join t2 on t1.uid = t2.uid ").print();


    }
}


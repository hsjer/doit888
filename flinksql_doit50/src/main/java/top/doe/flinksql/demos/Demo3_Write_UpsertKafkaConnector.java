package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo3_Write_UpsertKafkaConnector {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 创建源表
        tenv.executeSql("create table tpc_1 (\n" +
                "    id int,\n" +
                "    name string,\n" +
                "    gender string,\n" +
                "    score double\n" +
                ") with (\n" +
                "    'connector' = 'kafka',            \n" +
                "    'topic' = 'topic-1',              \n" +
                "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                "    'properties.group.id' = 'g001',       \n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json',              \n" +
                "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                ")");

        // 创建目标表
        tenv.executeSql("create table tpc_3 (            \n" +
                "    gender string                       \n" +
                "    ,score_amt double                   \n" +
                "    ,primary key(gender) NOT ENFORCED   \n" +
                ") with (                    \n" +
                "    'connector' = 'upsert-kafka',       \n" +
                "    'topic' = 'topic-3',              \n" +
                "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                "    'key.format' = 'json',       \n" +
                "    'value.format' = 'json',       \n" +
                "    'value.fields-include' = 'EXCEPT_KEY'\n" +
                ");\n");



        // 执行查询sql
        tenv.executeSql(
                "insert into tpc_3 \n" +
                "SELECT gender, sum(score) as score_amt\n" +
                "FROM tpc_1\n" +
                "GROUP BY gender");



    }
}

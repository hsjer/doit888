package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo2_KafkaConnector {
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
        tenv.executeSql("create table tpc_2 (\n" +
                "    id int,\n" +
                "    name string,\n" +
                "    gender string,\n" +
                "    score double\n" +
                ") with (\n" +
                "    'connector' = 'kafka',       \n" +
                "    'topic' = 'topic-2',              \n" +
                "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                "    'value.format' = 'json',       \n" +
                "    'value.fields-include' = 'EXCEPT_KEY'\n" +
                ")");


        // 执行查询
        tenv.executeSql(
                "insert into tpc_2 \n" +
                "select id,name,gender,score \n" +
                "from tpc_1\n" +
                "where score > 60");







    }
}

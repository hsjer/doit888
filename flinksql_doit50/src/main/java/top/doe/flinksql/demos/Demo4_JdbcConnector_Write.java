package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo4_JdbcConnector_Write {

    public static void main(String[] args) {


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
        tenv.executeSql(
                "create table score_mysql(\n" +
                        "    gender string,\n" +
                        "    score_amt double,\n" +
                        "    primary key(gender) not enforced\n" +
                        ") with (\n" +
                        "    'connector'='jdbc',\n" +
                        "    'url'='jdbc:mysql://doitedu01:3306/doit50',\n" +
                        "    'table-name'='t_score',\n" +
                        "    'username'='root',\n" +
                        "    'password'='ABC123.abc123'\n" +
                        ")");


        tenv.executeSql(
                "insert into score_mysql   " +
                "select gender,sum(score)  " +
                "from tpc_1                " +
                "group by gender           ");



    }

}

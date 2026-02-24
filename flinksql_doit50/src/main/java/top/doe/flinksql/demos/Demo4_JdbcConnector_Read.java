package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo4_JdbcConnector_Read {

    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 建表，读mysql的数据
        tenv.executeSql(
                "create table order_mysql(\n" +
                "    oid int,\n" +
                "    uid int,\n" +
                "    amt decimal(10,2),\n" +
                "    create_time timestamp(3),\n" +
                "    status int\n" +
                ") with (\n" +
                "    'connector'='jdbc',\n" +
                "    'url'='jdbc:mysql://doitedu01:3306/doit50',\n" +
                "    'table-name'='t_order',\n" +
                "    'username'='root',\n" +
                "    'password'='ABC123.abc123'\n" +
                ")");


        tenv.executeSql("select * from order_mysql").print();



    }

}

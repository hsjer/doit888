package top.doe.flinksql.demos;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.conf.HiveConf;

public class Demo22_JdbcCatalogDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);




        String name            = "jdbc";
        String defaultDatabase = "doit50";
        String username        = "root";
        String password        = "ABC123.abc123";
        String baseUrl         = "jdbc:mysql://doitedu01:3306/";
        // 构造一个jdbcCatalog对象
        JdbcCatalog jdbcCatalog = new JdbcCatalog(Demo22_JdbcCatalogDemo.class.getClassLoader(),
                name, defaultDatabase, username, password, baseUrl);

        // 注册catalog到 flinkSql的环境中
        tenv.registerCatalog("mysql_catalog",jdbcCatalog);


        // 可以直接访问mysql中的表
        tenv.executeSql("select * from mysql_catalog.doit50.t_order").print();


    }
}

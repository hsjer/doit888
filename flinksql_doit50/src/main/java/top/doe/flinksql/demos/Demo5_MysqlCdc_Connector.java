package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo5_MysqlCdc_Connector {

    public static void main(String[] args) {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);



        // 建表
        tenv.executeSql(
                        " create table order_cdc(              "+
                        "     oid int primary key not enforced,"+
                        "     uid int,                         "+
                        "     amt decimal(10,2),               "+
                        "     create_time timestamp(3),        "+
                        "     status int                       "+
                        " ) with (                             "+
                        "     'connector' = 'mysql-cdc',       "+
                        "     'hostname' = 'doitedu01',        "+
                        "     'port' = '3306',                 "+
                        "     'username' = 'root',             "+
                        "     'password' = 'ABC123.abc123',    "+
                        "     'database-name' = 'doit50',      "+
                        "     'table-name' = 't_order'         "+
                        " )                                    "
        );


        tenv.executeSql(
                        " create table status_amt(                         "+
                        "     status int,                                  "+
                        "     amt decimal(10,2),                           "+
                        "     primary key(status) not enforced             "+
                        " ) with (                                         "+
                        "     'connector'='jdbc',                          "+
                        "     'url'='jdbc:mysql://doitedu01:3306/doit50',  "+
                        "     'table-name'='status_amt',                   "+
                        "     'username'='root',                           "+
                        "     'password'='ABC123.abc123'                   "+
                        " )                                                "
        );


        // 计算
        tenv.executeSql(
                        " INSERT INTO status_amt         " +
                        " SELECT status,SUM(amt) AS amt  "+
                        " FROM order_cdc                 "+
                        " GROUP BY status                "
        );



    }


}

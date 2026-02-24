package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo6_Doris_Connector {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 建表，映射 mysql-cdc
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


        // 建表，映射 doris
        tenv.executeSql(
                        " create table status_amt_doris (                        "+
                        "   status int primary key not enforced,                 "+
                        "   amt decimal(10,2)                                    "+
                        " ) with (                                               "+
                        "     'connector' = 'doris',                             "+
                        "     'fenodes' = 'doitedu01:8030',                      "+
                        "     'table.identifier' = 'doit50.status_amt_unique',   "+
                        "     'username' = 'root',                               "+
                        "     'password' = '123456',                             "+
                        "     'sink.label-prefix' = 'doris_label'                "+
                        " )                                                      "
        );


        // 计算
        tenv.executeSql(
                        " INSERT INTO status_amt_doris   " +
                        " SELECT status,SUM(amt) AS amt  "+
                        " FROM order_cdc                 "+
                        " GROUP BY status                "
        );



    }


}

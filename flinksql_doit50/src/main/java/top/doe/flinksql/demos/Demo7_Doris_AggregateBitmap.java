package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo7_Doris_AggregateBitmap {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 建表，映射mysql-cdc
        tenv.executeSql(
                "create table order_cdc(             \n" +
                "    oid int,                        \n" +
                "    uid int,                        \n" +
                "    order_amt decimal(10,2),          \n" +
                "    status int ,                     \n" +
                "    province string ,                 \n" +
                "    city     string ,                 \n" +
                "    region   string ,                 \n" +
                "    primary key (oid) not enforced    \n" +
                ") with (                            \n" +
                "    'connector' = 'mysql-cdc',      \n" +
                "    'hostname' = 'doitedu01',       \n" +
                "    'port' = '3306',                \n" +
                "    'username' = 'root',            \n" +
                "    'password' = 'ABC123.abc123',   \n" +
                "    'database-name' = 'doit50',     \n" +
                "    'table-name' = 'order_2'        \n" +
                ")                                   ");

        // 建表，映射doris物理表
        tenv.executeSql(
                "create table order_ana(                       \n" +
                "    province varchar(20),                     \n" +
                "    city varchar(20),                         \n" +
                "    region varchar(20),                       \n" +
                "    status int,                               \n" +
                "    order_cnt bigint,                         \n" +
                "    order_amt decimal(10,2),                  \n" +
                "    user_id   int                             \n" +
                ") with (                                      \n" +
                "    'connector' = 'doris',                    \n" +
                "    'fenodes' = 'doitedu01:8030',             \n" +
                "    'table.identifier' = 'doit50.order_ana',  \n" +
                "    'username' = 'root',                      \n" +
                "    'password' = '123456',                    \n" +
                "    'sink.label-prefix' = 'doris_label_2'  ,  \n" +
                "    'sink.ignore.update-before' = 'false'  ,  \n" +
                "    'sink.properties.columns' = 'province,city,region,status,order_cnt,order_amt,user_id,user_bm=to_bitmap(user_id)'  ,\n" +
                "    'sink.properties.partial_columns' = 'true'\n" +
                ")");


        // 执行查询sql
        tenv.executeSql("insert into order_ana  \n" +
                "select province,city,region,status, 1 as order_cnt, order_amt,uid as user_id  \n" +
                "from order_cdc");

    }
}

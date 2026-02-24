package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/10/23
 * @Desc: 学大数据，上多易教育
 *  反面示例： tumble窗口聚合，不支持 changelog流（update/delete）
 **/
public class Demo9_Sql_TumbleWindow_Changelog {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //tenv.getConfig().set("table.exec.source.idle-timeout","10 ms");


        // 建表，映射 mysql的 cdc
        tenv.executeSql(
                " create table order_cdc(                                 "+
                        "     oid int primary key not enforced,           "+
                        "     uid int,                                    "+
                        "     amt decimal(10,2),                          "+
                        "     create_time timestamp(3),                   "+
                        "     status int,                                 "+
                        "     watermark for create_time as create_time    "+
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

        // 查询sql
        // TVF语法，会直接报错：
        // StreamPhysicalWindowAggregate doesn't support consuming update and delete changes
        // which is produced by node TableSourceScan(table=[[default_catalog, default_database, order_cdc]], fields=[oid, uid, amt, create_time, status])
        /* tenv.executeSql(
                " select window_start,window_end,sum(amt) as amt,count(distinct uid) as u_cnt "+
                        " from table(                                                                 "+
                        "     tumble(table order_cdc,descriptor(create_time),interval '5' minute)     "+
                        " )                                                                           "+
                        " group by window_start,window_end                                            "
        ).print(); */


        // GroupByWindow 语法 ，可以支持且处理changelog流的update/delete语义
        tenv.executeSql(
                        " select                                                    "+
                        "     tumble_start(create_time,interval '5' minute) as window_start, "+
                        "     tumble_end(create_time,interval '5' minute) as window_end,     "+
                        "                                                           "+
                        "     sum(amt) as amt,                                      "+
                        "     count(distinct uid) as u_cnt                          "+
                        "                                                           "+
                        " from order_cdc                                            "+
                        " group by tumble(create_time,interval '5' minute)                   "
        ).print();




    }
}

package top.doe.realdw.data_sync;

import org.apache.calcite.schema.StreamableTable;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UmsMember_2_Hbase {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        // 参数： execution.checkpointing.timeout = 默认 10min
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // execution.checkpointing.tolerable-failed-checkpoints = 0 未生效
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);


        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 建表,映射 mysql的binlog
        tenv.executeSql(
                " create table ums_member_mysql(        "+
                        "     id bigint                         "+
                        "     ,member_level_id bigint           "+
                        "     ,username string                  "+
                        "     ,password string                  "+
                        "     ,nickname string                  "+
                        "     ,phone string                     "+
                        "     ,status int                       "+
                        "     ,create_time timestamp(3)         "+
                        "     ,icon string                      "+
                        "     ,gender int                       "+
                        "     ,birthday date                    "+
                        "     ,city string                      "+
                        "     ,job string                       "+
                        "     ,personalized_signature string    "+
                        "     ,source_type int                  "+
                        "     ,integration int                  "+
                        "     ,growth int                       "+
                        "     ,luckey_count int                 "+
                        "     ,history_integration int          "+
                        "     ,modify_time timestamp(3)         "+
                        "     ,primary key(id) not enforced     "+
                        " ) WITH (                              "+
                        "     'connector' = 'mysql-cdc',        "+
                        "     'hostname' = 'doitedu01',         "+
                        "     'port' = '3306',                  "+
                        "     'username' = 'root',              "+
                        "     'password' = 'ABC123.abc123',     "+
                        "     'database-name' = 'realtime_dw',  "+
                        "     'table-name' = 'ums_member'       "+
                        " )                                     "
        );


        // 建表，映射目标表 : hbase中的 dim_user_info
        // hbase> create 'dim_user_info','f',SPLITS=>['c','g','m','p','u','x']
        tenv.executeSql(
                " create table dim_user_info_hbase(                  "+
                        "      username string                         "+
                        "      ,f  ROW<id bigint                       "+
                        "            ,member_level_id bigint           "+
                        "            ,password string                  "+
                        "            ,nickname string                  "+
                        "            ,phone string                     "+
                        "            ,status int                       "+
                        "            ,create_time timestamp(3)         "+
                        "            ,icon string                      "+
                        "            ,gender int                       "+
                        "            ,birthday date                    "+
                        "            ,city string                      "+
                        "            ,job string                       "+
                        "            ,personalized_signature string    "+
                        "            ,source_type int                  "+
                        "            ,integration int                  "+
                        "            ,growth int                       "+
                        "            ,luckey_count int                 "+
                        "            ,history_integration int          "+
                        "            ,modify_time timestamp(3)>        "+
                        "    ,primary key(username) not enforced       "+
                        " ) WITH (                                     "+
                        "  'connector' = 'hbase-2.2',                  "+
                        "  'table-name' = 'dim_user_info',             "+
                        "  'zookeeper.quorum' = 'doitedu01:2181'       "+
                        " )                                            "
        );


        // insert语句
        tenv.executeSql(
                "insert into dim_user_info_hbase " +
                        "select username," +
                        "row(id,member_level_id,password,nickname" +
                        ",phone,status,create_time,icon,gender,birthday,city,job," +
                        "personalized_signature,source_type,integration," +
                        "growth,luckey_count,history_integration,modify_time) as f " +
                        "from ums_member_mysql");


    }
}

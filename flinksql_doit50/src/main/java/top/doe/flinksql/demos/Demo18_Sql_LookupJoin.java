package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo18_Sql_LookupJoin {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.getConfig().set("table.exec.state.ttl","1 h");


        // 创建流表，映射kafka中的用户行为事件
        tenv.executeSql(
                "create table t1 (                              \n" +
                "    uid int,                                   \n" +
                "    event_id string,                           \n" +
                "    action_time bigint,                        \n" +
                "    pt as proctime(),                          \n" +  // 处理时间语义的时间字段
                "    rt as to_timestamp_ltz(action_time,3),     \n" +
                "    watermark for rt as rt                     \n" +
                ") with (                                       \n" +
                "    'connector' = 'kafka',                     \n" +
                "    'topic' = 'ss-1',                          \n" +
                "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                "    'properties.group.id' = 'g001',       \n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json',              \n" +
                "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                ")");

        // 创建hbase连接器表，映射用户注册信息
        tenv.executeSql(
                        " create table user_info(                                             "+
                        "     uid  string,                                                    "+
                        "     f    row<account string,gender string,member_level string>,    "+
                        "     primary key (uid) not enforced                                  "+
                        " ) with (                                                            "+
                        "     'connector' = 'hbase-2.2',                                      "+
                        "     'table-name' = 'user_info',                                     "+
                        "     'zookeeper.quorum' = 'doitedu01:2181',                          "+
                        "     'lookup.cache' = 'PARTIAL',                                     "+
                        "     'lookup.partial-cache.max-rows' = '100000',                     "+
                        "     'lookup.partial-cache.expire-after-access' = '30 m',            "+
                        "     'lookup.partial-cache.cache-missing-key' = 'false'              "+
                        " )                                                                   "
        );



        // lookup 关联
        tenv.executeSql(
                        " select                                                      "+
                        "     t1.uid                                                  "+
                        "     ,t1.event_id                                            "+
                        "     ,t1.action_time                                         "+
                        "     ,t2.f.account                                           "+
                        "     ,t2.f.gender                                            "+
                        "     ,t2.f.member_level                                      "+
                        " from t1 join user_info for system_time as of t1.pt as t2    "+
                        " on cast(t1.uid as string) = t2.uid                          "
        ).print();

    }
}


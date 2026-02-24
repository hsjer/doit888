package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.conf.HiveConf;

public class Demo22_HiveCatalogDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 构造一个hiveCatalog对象
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris","thrift://doitedu01:9083");
        HiveCatalog hiveCatalog = new HiveCatalog("hive", "default", hiveConf, "3.1.2");

        // 注册catalog到 flinkSql的环境中
        tenv.registerCatalog("doit50_catalog",hiveCatalog);


        // 直接这样建表，是建在默认的catalog中： GenericInMemoryCatalog（内存catalog）
        // default_catalog.default_database
        tenv.executeSql(
                "create table tpc_1 (\n" +
                "    id int,         \n" +
                "    name string,    \n" +
                "    gender string,  \n" +
                "    score double    \n" +
                ") with (            \n" +
                "    'connector' = 'kafka',            \n" +
                "    'topic' = 'topic-1',              \n" +
                "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                "    'properties.group.id' = 'g001',       \n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json',              \n" +
                "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                ")");

        // 显式指定目标 catalog
        // default在flinkSql中是关键字,注意飘
        tenv.executeSql(
                "create table `doit50_catalog`.`default`.`tpc_1`  (   \n" +
                        "    id int,                            \n" +
                        "    name string,                       \n" +
                        "    gender string,                     \n" +
                        "    score double                       \n" +
                        ") with (                               \n" +
                        "    'connector' = 'kafka',             \n" +
                        "    'topic' = 'topic-1',               \n" +
                        "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                        "    'properties.group.id' = 'g001',       \n" +
                        "    'scan.startup.mode' = 'latest-offset',\n" +
                        "    'value.format' = 'json',              \n" +
                        "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                        ")");

        // 先use，再默认
        tenv.executeSql("use catalog doit50_catalog");
        tenv.executeSql("create database db50");
        tenv.executeSql("use db50");
        tenv.executeSql(
                "create table tpc_1 (        \n" +
                        "    id int,         \n" +
                        "    name string,    \n" +
                        "    gender string,  \n" +
                        "    score double    \n" +
                        ") with (            \n" +
                        "    'connector' = 'kafka',            \n" +
                        "    'topic' = 'topic-1',              \n" +
                        "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                        "    'properties.group.id' = 'g001',       \n" +
                        "    'scan.startup.mode' = 'latest-offset',\n" +
                        "    'value.format' = 'json',              \n" +
                        "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                        ")");


        // 虽然上面有use，但是建表时显式指定了catalog和database，则用指定的
        // 其实这里指定的catalog:  GenericInMemoryCatalog
        tenv.executeSql(
                "create table default_catalog.default_database.tpc_3 (\n" +
                        "    id int,                               \n" +
                        "    name string,                          \n" +
                        "    gender string                         \n" +
                        ") with (                                  \n" +
                        "    'connector' = 'kafka',                \n" +
                        "    'topic' = 'topic-1',                  \n" +
                        "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                        "    'properties.group.id' = 'g001',       \n" +
                        "    'scan.startup.mode' = 'latest-offset',\n" +
                        "    'value.format' = 'json',              \n" +
                        "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                        ")");


        // 临时表，不放在任何的 catalog中，而是放在了catalogManager的hashmap中
        tenv.executeSql(
                "create temporary table tpc_3 (\n" +
                        "    id int,         \n" +
                        "    name string,    \n" +
                        "    gender string,  \n" +
                        "    score double    \n" +
                        ") with (            \n" +
                        "    'connector' = 'kafka',            \n" +
                        "    'topic' = 'topic-1',              \n" +
                        "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                        "    'properties.group.id' = 'g002',       \n" +
                        "    'scan.startup.mode' = 'latest-offset',\n" +
                        "    'value.format' = 'json',              \n" +
                        "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                        ")");


        tenv.executeSql("select * from tpc_3").print();


    }
}

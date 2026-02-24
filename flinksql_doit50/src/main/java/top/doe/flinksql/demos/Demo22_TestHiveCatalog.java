package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.conf.HiveConf;

public class Demo22_TestHiveCatalog {
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


        // 查询 hiveCatalog.db50.tpc_1
        tenv.executeSql("select * from doit50_catalog.db50.tpc_1").print();


    }
}

package top.doe.hive.localtest;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.Test;
import top.doe.hive.udf.templates.exec.IntGroup2BitmapUDAF;
import top.doe.hive.udf.templates.exec.MyAvg;

public class MyUDFIntegrationTest {
    private Driver driver;

    @Before
    public void setup() throws HiveException {
        HiveConf conf = new HiveConf();
        conf.set("fs.defaultFS","hdfs://doitedu01:8020/");
        conf.set("hive.metastore.uris","thrift://doitedu01:9083");

        SessionState.start(new SessionState(conf));
        driver = new Driver(conf);

        // 注册自定义函数
        FunctionRegistry.registerTemporaryUDF("bmagg", IntGroup2BitmapUDAF.class);
//        FunctionRegistry.registerTemporaryUDF("myavg", MyAvg.class);
    }

    @Test
    public void testUDFInHive() throws HiveException {
        // String query = "select  user_name,bmagg(id)  from orders group by user_name";
        // String query = "select  user_name,myavg(id)  from orders group by user_name";

        String query = "select a3,count(1) from d2.events group by a3";


        CommandProcessorResponse run = driver.run(query);

        System.out.println(run);
        // 验证查询结果
    }
}


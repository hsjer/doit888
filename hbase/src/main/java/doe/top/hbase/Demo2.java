package doe.top.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Demo2 {

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","doitedu01:2181");

        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("order"));

        // oid,uid,  amt,  pid, quantity
        // 1L,  1L,  88.8,  p01,   5
        // 先把待插入的数据封装成put对象
        Put put1 = new Put(Bytes.toBytes(1L));
        put1.addColumn(Bytes.toBytes("f"),Bytes.toBytes("uid"),Bytes.toBytes(1L));
        put1.addColumn(Bytes.toBytes("f"),Bytes.toBytes("amt"),Bytes.toBytes(88.8));
        put1.addColumn(Bytes.toBytes("f"),Bytes.toBytes("pid"),Bytes.toBytes("p01"));
        put1.addColumn(Bytes.toBytes("f"),Bytes.toBytes("quantity"),Bytes.toBytes(5));



        Put put2 = new Put(Bytes.toBytes(2L));
        put2.addColumn(Bytes.toBytes("f"),Bytes.toBytes("uid"),Bytes.toBytes(2L));
        put2.addColumn(Bytes.toBytes("f"),Bytes.toBytes("amt"),Bytes.toBytes(78.8));
        put2.addColumn(Bytes.toBytes("f"),Bytes.toBytes("pid"),Bytes.toBytes("p11"));
        put2.addColumn(Bytes.toBytes("f"),Bytes.toBytes("quantity"),Bytes.toBytes(2));



        Put put3 = new Put(Bytes.toBytes(3L));
        put3.addColumn(Bytes.toBytes("f"),Bytes.toBytes("uid"),Bytes.toBytes(3L));
        put3.addColumn(Bytes.toBytes("f"),Bytes.toBytes("amt"),Bytes.toBytes(98.8));
        put3.addColumn(Bytes.toBytes("f"),Bytes.toBytes("pid"),Bytes.toBytes("p21"));
        put3.addColumn(Bytes.toBytes("f"),Bytes.toBytes("quantity"),Bytes.toBytes(1));



        // 调用put方法，这准备好的数据发给hbase服务端
        //table.put(put1);
        //table.put(put2);
        //table.put(put3);

        List<Put> putList = Arrays.asList(put1, put2, put3);
        table.put(putList);

        table.close();
        conn.close();

    }

}

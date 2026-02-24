package doe.top.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Demo3 {
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","doitedu01:2181");

        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("order"));


        // 读取订单1的数据
        Get get = new Get(Bytes.toBytes(1L));

        Result result = table.get(get);
        // 取值
        // uid,  amt,  pid, quantity
        byte[] uidBytes = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("uid"));
        byte[] amtBytes = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("amt"));
        byte[] pidBytes = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("pid"));
        byte[] quantityBytes = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("quantity"));

        long uid = Bytes.toLong(uidBytes);
        double amt = Bytes.toDouble(amtBytes);
        String pid = Bytes.toString(pidBytes);
        int quantity = Bytes.toInt(quantityBytes);

        System.out.println(uid);
        System.out.println(amt);
        System.out.println(pid);
        System.out.println(quantity);

        table.close();
        conn.close();

    }

}

package doe.top.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/29
 * @Desc: 学大数据，上多易教育
 *
 **/
public class Demo4 {
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","doitedu01:2181");

        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("order"));

        // 扫描行键范围内的数据
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(1L),true);
        scan.withStopRow(Bytes.toBytes(10L),true);

        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){

            // oid, uid,  amt,  pid, quantity
            Result result = iterator.next();

            byte[] rowkeyBytes = result.getRow();
            byte[] uidBytes = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("uid"));
            byte[] amtBytes = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("amt"));
            byte[] pidBytes = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("pid"));
            byte[] quantityBytes = result.getValue(Bytes.toBytes("f"), Bytes.toBytes("quantity"));

            long oid = Bytes.toLong(rowkeyBytes);
            long uid = Bytes.toLong(uidBytes);
            double amt = Bytes.toDouble(amtBytes);
            String pid = Bytes.toString(pidBytes);
            int quantity = Bytes.toInt(quantityBytes);

            System.out.println(String.format("oid:%d,uid:%d,amt:%.2f,pid:%s,quantity:%d",oid,uid,amt,pid,quantity));


        }

        table.close();
        conn.close();


    }
}

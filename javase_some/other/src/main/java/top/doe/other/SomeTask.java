package top.doe.other;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class SomeTask {
    public void run(int cnt) throws InterruptedException {
        for (int i = 0; i < cnt; i++) {
            System.out.println("SomeTask.run ==> " + i);
            System.err.println("SomeTask.run.err ==> " + i);
            Thread.sleep(500);
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","doitedu01:2181");
        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf("t1"));
        Result result = table.get(new Get("r1".getBytes()));
        CellScanner cellScanner = result.cellScanner();
        while(cellScanner.advance()){
            Cell cell = cellScanner.current();
            byte[] rowBytes = CellUtil.cloneRow(cell);
            byte[] fBytes = CellUtil.cloneFamily(cell);
            byte[] cBytes = CellUtil.cloneQualifier(cell);
            byte[] vBytes = CellUtil.cloneValue(cell);

            System.out.println(new String(rowBytes));
            System.out.println(new String(fBytes));
            System.out.println(new String(cBytes));
            System.out.println(new String(vBytes));
        }
    }
}

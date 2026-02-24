package doe.top.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/29
 * @Desc: 学大数据，上多易教育
 *   hbase的客户端编程示例
 **/
public class Demo1 {

    public static void main(String[] args) throws IOException {

        // 准备连接参数
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","doitedu01:2181,doitedu02:2181,doitedu03:2181");

        // 创建连接
        Connection conn = ConnectionFactory.createConnection(conf);

        // ddl
        Admin admin = conn.getAdmin();

        // DML
        Table table = conn.getTable(TableName.valueOf("boys"));

        // 指定get的目标行键
        Get getParam = new Get("boy001".getBytes());
        getParam.addColumn("f1".getBytes(),"name".getBytes());
        getParam.addColumn("f1".getBytes(),"age".getBytes());

        // 调用get方法来查询
        Result result = table.get(getParam);

        // 如果知道这一行中返回的上哪些字段，可以这样直接取value
        byte[] nameBytes = result.getValue("f1".getBytes(), "name".getBytes());
        byte[] ageBytes = result.getValue("f1".getBytes(), "age".getBytes());

        System.out.println("name: " + new String(nameBytes) + ", age: " + new String(ageBytes));
        //System.exit(1);


        // 如果不知道这一行中返回的有哪些字段，可以这样迭代
        CellScanner cellScanner = result.cellScanner();
        while(cellScanner.advance()){
            Cell cell = cellScanner.current();
            byte[] rowKeyBytes = CellUtil.cloneRow(cell);
            byte[] familyBytes = CellUtil.cloneFamily(cell);
            byte[] qualifierBytes = CellUtil.cloneQualifier(cell);
            byte[] valueBytes = CellUtil.cloneValue(cell);

            String rowkey = new String(rowKeyBytes);
            String family = new String(familyBytes);
            String qualifier = new String(qualifierBytes);
            String value = new String(valueBytes);

            System.out.println(String.format("行键:%s,列族:%s,qualifier:%s,value:%s",rowkey,family,qualifier,value));

        }

        table.close();
        conn.close();

    }


}

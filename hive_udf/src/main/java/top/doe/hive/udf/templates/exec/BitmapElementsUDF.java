package top.doe.hive.udf.templates.exec;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/7
 * @Desc: 学大数据，上多易教育
 *   打印bitmap的元素
 **/
public class BitmapElementsUDF   extends UDF {

    public String evaluate(BytesWritable bitmapBytesWritable) throws IOException {

        // 反序列化
        RoaringBitmap bitmap = BitmapUtil.deBitmap(bitmapBytesWritable.getBytes());

        return bitmap.toString().replaceAll("\\{","\\[").replaceAll("\\}","]");
    }

}

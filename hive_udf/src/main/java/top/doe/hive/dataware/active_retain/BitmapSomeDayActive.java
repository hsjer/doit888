package top.doe.hive.dataware.active_retain;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/8
 * @Desc: 学大数据，上多易教育
 *  指定一些日期，是否都活跃
 **/
public class BitmapSomeDayActive extends UDF {
    public boolean evaluate(BytesWritable bytesWritable,String... dt) throws IOException {
        LocalDate d0 = LocalDate.parse("1970-01-01");

        Roaring64Bitmap bitmap = BitmapSerDe.deSer(bytesWritable.getBytes());

        for (String d : dt) {
            LocalDate d1 = LocalDate.parse(d);
            long diff = ChronoUnit.DAYS.between(d0, d1);
            if(!bitmap.contains(diff)) return false;
        }

        return true;
    }
}

package top.doe.hive.dataware.active_retain;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class DateArray2BitmapFunction extends UDF {

    public BytesWritable evaluate(String... dt) throws IOException {
        LocalDate d0 = LocalDate.parse("1970-01-01");

        Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf();

        for (String d : dt) {
            // 将字符串转换为 LocalDate
            LocalDate d2 = LocalDate.parse(d);

            // 计算天数差
            long daysBetween = ChronoUnit.DAYS.between(d0, d2);
            bitmap.add(daysBetween);

        }

        // 把bitmap序列化
        byte[] ser = BitmapSerDe.ser(bitmap);
        return new BytesWritable(ser);
    }


}

package top.doe.hive.dataware.active_retain;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class BitmapAddDateFunction extends UDF {

    public BytesWritable evaluate(BytesWritable bmBytesWritable,String... dt) throws IOException {
        // 把输入bitmap字节反序列化
        Roaring64Bitmap bitmap = BitmapSerDe.deSer(bmBytesWritable.getBytes());


        LocalDate d0 = LocalDate.parse("1970-01-01");
        for (String d : dt) {
            LocalDate d2 = LocalDate.parse(d);
            long daysBetween = ChronoUnit.DAYS.between(d0, d2);

            bitmap.add(daysBetween);
        }

        // 把结果bitmap序列化
        byte[] bytes = BitmapSerDe.ser(bitmap);
        return new BytesWritable(bytes);
    }

}

package top.doe.hive.dataware.active_retain;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class BitmapShowDateFunction extends UDF {

    public String evaluate(BytesWritable bitmapBytes) throws IOException {
        LocalDate d0 = LocalDate.parse("1970-01-01");
        // 反序列化
        Roaring64Bitmap bitmap = BitmapSerDe.deSer(bitmapBytes.getBytes());

        long[] days = bitmap.toArray();

        StringBuilder sb = new StringBuilder();

        for (long d : days) {
            LocalDate localDate = d0.plusDays(d);
            sb.append(localDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))).append(",");
        }

        return sb.toString();

    }

}

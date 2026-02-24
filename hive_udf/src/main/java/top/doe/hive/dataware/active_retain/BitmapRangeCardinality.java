package top.doe.hive.dataware.active_retain;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class BitmapRangeCardinality extends UDF {

    // 求指定日期范围内，用户的活跃天数
    public int evaluate(BytesWritable bmBytesWritable , String rangeStartDt, String rangeEndDt ) throws IOException {
        LocalDate d0 = LocalDate.parse("1970-01-01");

        LocalDate d1 = LocalDate.parse(rangeStartDt);

        LocalDate d2 = LocalDate.parse(rangeEndDt);

        long start = ChronoUnit.DAYS.between(d0, d1);
        long end = ChronoUnit.DAYS.between(d0, d2) + 1;

        Roaring64Bitmap maskBitmap = Roaring64Bitmap.bitmapOf();
        maskBitmap.addRange(start,end);

        // 把遮罩bitmap，跟 输入的bitmap做与操作
        Roaring64Bitmap bitmap = BitmapSerDe.deSer(bmBytesWritable.getBytes());
        bitmap.and(maskBitmap);


        return bitmap.getIntCardinality();
    }


    public static void main(String[] args) {

        Roaring64Bitmap bm = Roaring64Bitmap.bitmapOf(21, 22, 25, 28, 32, 38, 40, 46, 60);

        Roaring64Bitmap mask = Roaring64Bitmap.bitmapOf();
        mask.addRange(22,41);

        bm.and(mask);

        System.out.println(bm.getIntCardinality());
    }
}

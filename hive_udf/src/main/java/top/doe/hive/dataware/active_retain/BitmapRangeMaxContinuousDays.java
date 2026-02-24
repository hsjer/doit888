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
 *  求指定日期范围内的，用户最大连续活跃天数
 **/
public class BitmapRangeMaxContinuousDays extends UDF {


    public int evaluate(BytesWritable bmBytesWritable, String rangeStartDt, String rangeEndDt  ) throws IOException {
        LocalDate d0 = LocalDate.parse("1970-01-01");
        LocalDate d1 = LocalDate.parse(rangeStartDt);
        LocalDate d2 = LocalDate.parse(rangeEndDt);

        long start = ChronoUnit.DAYS.between(d0, d1);
        long end = ChronoUnit.DAYS.between(d0, d2) + 1;

        Roaring64Bitmap mask = Roaring64Bitmap.bitmapOf();
        mask.addRange(start,end);

        // 反序列化bitmap
        Roaring64Bitmap bitmap = BitmapSerDe.deSer(bmBytesWritable.getBytes());
        bitmap.and(mask);


        long[] array = bitmap.toArray();
        int max = 0;
        int c=1;
        for (int j = 0; j < array.length-1; j++) {
            if(array[j+1] - array[j] == 1 ) {
                c++;
            }else{
                max = Math.max(max,c);
                c=1;
            }
        }

        max = Math.max(max,c);

        return array.length<=1 ? array.length: max;

    }



    public static void main(String[] args) {

        // 20 ~  50 之间的最大连续活跃天数
        Roaring64Bitmap bm = Roaring64Bitmap.bitmapOf(18,21, 22, 23, 24, 32, 33, 34, 36, 37,38,42,48,52,56,60);

        Roaring64Bitmap mask = Roaring64Bitmap.bitmapOf();
        mask.addRange(20,51);
        bm.and(mask);

        long[] array = bm.toArray();
        int max = 0;
        int c=1;
        for (int j = 0; j < array.length-1; j++) {
            if(array[j+1] - array[j] == 1 ) {
                c++;
            }else{
               max = Math.max(max,c);
               c=1;
            }
        }
        System.out.println(max);

    }

}

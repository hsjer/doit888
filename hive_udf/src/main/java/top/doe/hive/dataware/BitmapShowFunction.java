package top.doe.hive.dataware;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/5
 * @Desc: 学大数据，上多易教育
 *   求一个bitmap的基数
 **/
public class BitmapShowFunction extends UDF {

    public String evaluate(String bitmapStr,int length) throws IOException {
        byte[] bytes = Base64.decodeBase64(bitmapStr);
        Roaring64Bitmap bm = Roaring64Bitmap.bitmapOf();
        bm.deserialize(ByteBuffer.wrap(bytes));

        String string = bm.toString();
        if(string.length()>length){
            string=string.substring(0,length)+"...";
        }

        return string;
    }

}

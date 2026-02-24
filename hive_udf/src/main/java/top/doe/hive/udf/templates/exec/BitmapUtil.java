package top.doe.hive.udf.templates.exec;

import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BitmapUtil {


    // bitmap的序列化工具方法
    public static byte[] serBitmap(RoaringBitmap bitmap) throws IOException {
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bas);
        bitmap.serialize(dout);

        return bas.toByteArray();
    }


    // bitmap的反序列化工具方法
    public static RoaringBitmap deBitmap(byte[] bytes) throws IOException {
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
        bitmap.deserialize(ByteBuffer.wrap(bytes));

        return bitmap;
    }
}

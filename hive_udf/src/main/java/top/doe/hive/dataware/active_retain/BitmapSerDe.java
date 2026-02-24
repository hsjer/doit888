package top.doe.hive.dataware.active_retain;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BitmapSerDe {
    public static byte[] ser(Roaring64Bitmap bitmap) throws IOException {

        ByteArrayOutputStream ba = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(ba);
        bitmap.serialize(dout);

        return ba.toByteArray();

    }



    public static Roaring64Bitmap deSer(byte[] data) throws IOException {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        bitmap.deserialize(ByteBuffer.wrap(data));

        return bitmap;
    }
}

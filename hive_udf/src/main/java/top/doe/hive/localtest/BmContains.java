package top.doe.hive.localtest;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BmContains extends UDF {

    public boolean evaluate(byte[] bmBytes,int ele){
        RoaringBitmap bm = RoaringBitmap.bitmapOf();
        try {
            bm.deserialize(ByteBuffer.wrap(bmBytes));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return bm.contains(ele);

    }

}

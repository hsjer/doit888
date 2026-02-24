package top.doe.hive.localtest;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BitmapUDAF extends UDAF {

    public static class BmEvaluator implements UDAFEvaluator {

        private RoaringBitmap agg;

        public BmEvaluator() {
            super();
            init();
        }

        @Override
        public void init() {
            agg = RoaringBitmap.bitmapOf();
        }

        public boolean iterate(Integer value) {
            agg.add(value);
            return true;
        }

        public BytesWritable terminatePartial() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                agg.serialize(new DataOutputStream(baos));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            byte[] ba = baos.toByteArray();

            return new BytesWritable(ba);
        }


        public boolean merge(BytesWritable partialBmBytes) throws IOException {

            RoaringBitmap bmPartial = RoaringBitmap.bitmapOf();
            bmPartial.deserialize(ByteBuffer.wrap(partialBmBytes.getBytes()));

            agg.or(bmPartial);

            return true;
        }

        public byte[] terminate() throws IOException {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            agg.serialize(new DataOutputStream(baos));

            return baos.toByteArray();
        }

    }

}

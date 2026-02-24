package top.doe.hive.udf.templates.exec;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

import static top.doe.hive.udf.templates.exec.BitmapUtil.serBitmap;
import static top.doe.hive.udf.templates.exec.BitmapUtil.deBitmap;

public class IntGroup2BitmapUDAF extends UDAF {

    public static class BmEvaluator implements UDAFEvaluator {

        RoaringBitmap bm;

        public BmEvaluator(){
            init();
        }

        @Override
        public void init() {
            bm = RoaringBitmap.bitmapOf();
        }

        public boolean iterate(Integer id) {
            bm.add(id);
            return true;
        }


        public BytesWritable terminatePartial() throws IOException {
            // 把bitmap序列化成字节
            byte[] bytes = serBitmap(this.bm);

            // 把序列化的结果字节，包装成 BytesWritable 输出
            return new BytesWritable(bytes);
        }


        public boolean merge(BytesWritable writable) throws IOException {
            // 把序列化后的局部聚合bitmap字节，反序列化成bitmap对象
            RoaringBitmap partialBitmap = deBitmap(writable.getBytes());

            //  将接收到的局部聚合bitmap，合并到自己的累加器bitmap上
            this.bm.or(partialBitmap);

            return true;
        }


        public BytesWritable terminate() throws IOException {

            // 实际要返回的是合并好的 bitmap
            byte[] bytes = serBitmap(this.bm);

            return new BytesWritable(bytes);
        }
    }
}

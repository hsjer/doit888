package top.doe.hive.udf.templates.exec;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

import static top.doe.hive.udf.templates.exec.BitmapUtil.deBitmap;
import static top.doe.hive.udf.templates.exec.BitmapUtil.serBitmap;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/7
 * @Desc: 学大数据，上多易教育
 *   聚合函数
 *   将一组 bitmap 合并为一个 bitmap
 **/
public class BitmapAggUDAF extends UDAF {

    public static class BmEvaluator implements UDAFEvaluator {

        RoaringBitmap bm;

        public BmEvaluator(){
            init();
        }

        @Override
        public void init() {
            bm = RoaringBitmap.bitmapOf();
        }

        public boolean iterate(BytesWritable bmBytesWritable) throws IOException {
            // 反序列化输入的bitmap
            RoaringBitmap bitmap = deBitmap(bmBytesWritable.getBytes());

            // 累加到自己的累加器
            this.bm.or(bitmap);

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

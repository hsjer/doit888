package top.doe.hive.dataware;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/5
 * @Desc: 学大数据，上多易教育
 *   聚合一组 长整数  为一个bitmap
 **/
public class Bigint2BitmapAggFunction extends UDAF {

    public static class BitmapAggEvaluator implements UDAFEvaluator {

        Roaring64Bitmap roaring64Bitmap;
        public BitmapAggEvaluator(){
            super();
            init();
        }


        @Override
        public void init() {
            roaring64Bitmap = Roaring64Bitmap.bitmapOf();
        }

        // 对输入数据进行计算
        public boolean iterate(Long id) {
            roaring64Bitmap.add(id);
            return true;
        }

        // 返回可能的局部聚合结果
        public String terminatePartial() throws IOException {
            // 序列化bitmap
            ByteArrayOutputStream baOut = new ByteArrayOutputStream();
            DataOutputStream dOut = new DataOutputStream(baOut);

            roaring64Bitmap.serialize(dOut);

            byte[] bytes = baOut.toByteArray();

            String base64String = Base64.encodeBase64String(bytes);

            return base64String;
        }

        // 合并各个 聚合聚合结果
        public boolean merge(String partialBitmapStr) throws IOException {

            byte[] bytes = Base64.decodeBase64(partialBitmapStr);
            Roaring64Bitmap bm = Roaring64Bitmap.bitmapOf();
            bm.deserialize(ByteBuffer.wrap(bytes));

            // 合并
            roaring64Bitmap.or(bm);

            return true;
        }

        // 返回 最终的输出结果
        public String terminate() throws IOException {
            // 序列化bitmap
            ByteArrayOutputStream baOut = new ByteArrayOutputStream();
            DataOutputStream dOut = new DataOutputStream(baOut);

            roaring64Bitmap.serialize(dOut);

            byte[] bytes = baOut.toByteArray();

            String base64String = Base64.encodeBase64String(bytes);

            return base64String;

        }



    }
}

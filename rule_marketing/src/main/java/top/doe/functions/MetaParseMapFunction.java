package top.doe.functions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import top.doe.bean.RuleMeta;

import java.nio.ByteBuffer;

public class MetaParseMapFunction implements MapFunction<String, RuleMeta> {
    @Override
    public RuleMeta map(String json) throws Exception {

        JSONObject jsonObject = JSON.parseObject(json);
        String op = jsonObject.getString("op");

        // 获取cdc数据中的after：包含表数据
        JSONObject data;
        if (op.equals("d")) {
            data = jsonObject.getJSONObject("before");
        } else {
            data = jsonObject.getJSONObject("after");
        }

        // 把数据，封装到RuleMeta对象中
        RuleMeta ruleMeta = JSON.parseObject(data.toJSONString(), RuleMeta.class);
        // 从中取出bitmap字节，反序列化成 bitmap对象
        byte[] ruleCrowdBitmapBytes = ruleMeta.getRule_crowd_bitmap_bytes();
        Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf();
        bitmap.deserialize(ByteBuffer.wrap(ruleCrowdBitmapBytes));

        // 再把bitmap对象，和op，放入 ruleMeta中
        ruleMeta.setRule_crowd_bitmap(bitmap);
        ruleMeta.setRule_crowd_bitmap_bytes(null);

        ruleMeta.setOp(op);

        return ruleMeta;
    }
}

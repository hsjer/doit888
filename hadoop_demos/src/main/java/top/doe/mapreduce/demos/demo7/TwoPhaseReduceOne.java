package top.doe.mapreduce.demos.demo7;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoPhaseReduceOne extends Reducer<Text, DoubleWritable,Text, NullWritable> {

    @Override  // "省\001随机数"
    protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        String[] split = key.toString().split("\001");
        String province = split[0];

        double sum = 0;
        int cnt = 0;

        // 累计金额和个数
        for (DoubleWritable value : values) {
            sum += value.get();
            cnt++;
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("province",province);
        jsonObject.put("order_amount",sum);
        jsonObject.put("order_count",cnt);

        context.write(new Text(jsonObject.toJSONString()),NullWritable.get());

    }
}

package top.doe.mapreduce.demos.demo6;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HighReducer extends Reducer<OrderBean, NullWritable, Text,NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Reducer<OrderBean, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        // 因为我们的key有compare()方法，是按 用户id排，用户id相同则按金额大小排
        // 所以，到reduce的时候，这个数据的顺序是：
        // {1,200}:NW {1,160}:NW {1,120}:NW   {3,300}:NW {3,200}:NW {3,100}:NW
        // 而且，我有自定义了一个分组比较器，那么这些订单数据中，只要用户id相同的，就会被认为是同一组

        int i = 0;
        // 虽然这个迭代器看起来只是在迭代value
        // 其实它每得迭代一次，都会获得一对KV的数据，它会把获得 k的数据set到方法的入参对象  key 中
        // 然后把 获得的 v 大数据，set到 迭代器的返回对象value中
        for (NullWritable value : values) {
            context.write(new Text(JSON.toJSONString(key)),value);
            i++;

            if(i==2) break;
        }

    }
}

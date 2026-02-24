package top.doe.mapreduce.demos.demo3.agg;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import top.doe.mapreduce.demos.demo3.join.OrderUser;

import java.io.IOException;

public class OrderAggReducer extends Reducer<Text, OrderUser, NullWritable,Text> {

    @Override
    protected void reduce(Text key, Iterable<OrderUser> values, Reducer<Text, OrderUser, NullWritable, Text>.Context context) throws IOException, InterruptedException {

        // 订单总额
        double amount = 0 ;
        // 订单总数
        int count = 0;

        for (OrderUser value : values) {
            amount += value.getAmount();
            count++;
        }

        OrderAgg orderAgg = new OrderAgg(key.toString(), amount, count);
        String aggJson = JSON.toJSONString(orderAgg);

        context.write(NullWritable.get(), new Text(aggJson));

    }
}

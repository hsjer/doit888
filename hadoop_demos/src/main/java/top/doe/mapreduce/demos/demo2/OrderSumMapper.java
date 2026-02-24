package top.doe.mapreduce.demos.demo2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderSumMapper extends Mapper<LongWritable, Text, IntWritable,Order> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Order>.Context context) throws IOException, InterruptedException {

        // {"order_id":"o1","member_id":1001,"amount":120.8,"receive_address":"北京","date":"2024-08-1"}

        // 解析json
        Order order = JSON.parseObject(value.toString(), Order.class);

        // 提取 用户id做key， 解析的对象做 value
        int memberId = order.getMember_id();
        context.write(new IntWritable(memberId),order);

    }
}

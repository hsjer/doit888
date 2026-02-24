package top.doe.mapreduce.demos.demo6;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HighMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context) throws IOException, InterruptedException {

        OrderBean orderBean = JSON.parseObject(value.toString(), OrderBean.class);
        context.write(orderBean,NullWritable.get());

    }
}

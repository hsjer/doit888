package top.doe.mapreduce.demos.demo1;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Demo1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // key: 一行数据的起始偏移量
        // value: 一行数据的内容   "hello ,tom ,are you ok,how are you today?"
        // 输出:  <hello,1> <tom,1> , <are,1>  ......

        String line = value.toString();
        String[] words = line.split("[\\?.,\\s]+");

        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }

        Thread.sleep(10000);

    }


}

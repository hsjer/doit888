package top.doe.mapreduce.demos.demo8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WcReducer extends Reducer<Text, IntWritable, Text, ShortWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, ShortWritable>.Context context) throws IOException, InterruptedException {

        short count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        context.write(key, new ShortWritable(count));

    }
}

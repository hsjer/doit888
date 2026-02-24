package top.doe.mapreduce.demos.demo7;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TwoPhaseMapperOne extends Mapper<LongWritable, Text,Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        // o1,u1,江苏省,扬州市,280.0
        String[] split = value.toString().split(",");
        String province = split[2];
        double money = Double.parseDouble(split[4]);

        // 弄个随机数
        int random = (int)(Math.random()*100);
        int numReduceTasks = context.getNumReduceTasks();

        int groupRand = random % numReduceTasks;

        //
        context.write(new Text(province+"\001"+groupRand),new DoubleWritable(money));

    }
}

package top.doe.mapreduce.demos.demo7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class TwoPhaseJobOneMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        //conf.set("mapreduce.framework.name","local");   // 本地模式
        conf.set("fs.defaultFS","file:///"); // 本地文件系统

        Job job = Job.getInstance(conf);

        //job.setJarByClass(TwoPhaseJobOneMain.class);

        job.setMapperClass(TwoPhaseMapperOne.class);
        job.setReducerClass(TwoPhaseReduceOne.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job,"file:///d:/orders/input");

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///d:/order/output_1/"));

        job.setNumReduceTasks(3);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}

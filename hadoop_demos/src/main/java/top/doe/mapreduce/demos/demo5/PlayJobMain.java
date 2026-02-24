package top.doe.mapreduce.demos.demo5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class PlayJobMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        //conf.set("mapreduce.framework.name","local");   // 本地模式
        conf.set("fs.defaultFS","file:///"); // 本地文件系统
        conf.set("mapreduce.input.fileinputformat.split.maxsize","128m");
        conf.set("mapreduce.input.fileinputformat.split.minsize","128m");


        Job job = Job.getInstance(conf);

        job.setJarByClass(PlayJobMain.class);

        job.setMapperClass(PlayMapper.class);
        job.setReducerClass(PlayReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PlayBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job,"file:///d:/events/input");

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///d:/events/output"));

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true)?0:1);

    }
}

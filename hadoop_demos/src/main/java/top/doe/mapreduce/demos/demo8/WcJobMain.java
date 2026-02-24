package top.doe.mapreduce.demos.demo8;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;

public class WcJobMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {


        FileUtils.deleteDirectory(new File("d:/wordcount/output"));

        Configuration conf = new Configuration();

        // job对象： mapreduce框架提供的作业提交器
        Job job = Job.getInstance(conf);

        job.setJarByClass(WcJobMain.class);

        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ShortWritable.class);

        //job.setInputFormatClass(CombineTextInputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("file:///d:/wordcount/input/"));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///d:/wordcount/output"));

        // 设置reduceTask的并行度
        job.setNumReduceTasks(2);

        // 提交作业，并等待作业完成
        System.exit(job.waitForCompletion(true)? 0 :1);
    }
}

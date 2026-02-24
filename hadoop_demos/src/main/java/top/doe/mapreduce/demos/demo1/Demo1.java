package top.doe.mapreduce.demos.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Demo1 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
/*
        conf.set("fs.defaultFS", "hdfs://doitedu01:8020/");
        conf.set("mapreduce.framework.name","yarn");
        conf.set("yarn.resourcemanager.hostname","doitedu01");
*/

        // job对象： mapreduce 框架提供的作业提交器
        Job job = Job.getInstance(conf);

        // 设置作业的jar
        job.setJarByClass(Demo1.class);
        //job.setJar("/root/abc/xxx.jar");

        job.setMapperClass(Demo1Mapper.class);
        job.setReducerClass(Demo1Reducer.class);

        // 为 mapTask 添加本地局部聚合逻辑类
        //job.setCombinerClass(Demo1Reducer.class);

        // map输出的是：单词,1
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入格式
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputDirRecursive(job,true);
        TextInputFormat.addInputPath(job, new Path("/wordcount/input/"));

        // 设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(args[1]));

        // 设置reduceTask的并行度
        //job.setNumReduceTasks(2);


        // 提交后直接退出
        // job.submit();

        // 提交作业，并等待作业完成
        System.exit(job.waitForCompletion(true)? 0 :1);


    }
}

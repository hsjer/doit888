package top.doe.mapreduce.demos.demo3.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class JoinJobMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(JoinJobMain.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(OrderUser.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrderUser.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job,"/order/input,/order_user/input");

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,new Path("/order_join/output"));

//        job.setOutputFormatClass(TextOutputFormat.class);
//        TextOutputFormat.setOutputPath(job,new Path("/order_join/output"));

        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true)?0:1);


    }
}

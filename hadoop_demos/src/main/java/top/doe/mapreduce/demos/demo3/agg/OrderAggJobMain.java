package top.doe.mapreduce.demos.demo3.agg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import top.doe.mapreduce.demos.demo3.join.OrderUser;

import java.io.IOException;

public class OrderAggJobMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderAggJobMain.class);

        job.setMapperClass(OrderAggMapper.class);
        job.setReducerClass(OrderAggReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderUser.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPaths(job,"/order_join/output");

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("/order_agg/output"));

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true)?0:1);


    }
}

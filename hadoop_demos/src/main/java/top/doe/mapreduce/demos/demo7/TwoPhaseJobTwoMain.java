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

public class TwoPhaseJobTwoMain {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(TwoPhaseMapperTwo.class);
        job.setReducerClass(TwoPhaseReducerTwo.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderAggBean.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job,"file:///d:/order/output_1/");

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///d:/order/output_2/"));


        job.setNumReduceTasks(3);

        System.exit(job.waitForCompletion(true)?0:1);
    }
}

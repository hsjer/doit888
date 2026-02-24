package top.doe.mapreduce.demos.demo4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapSideJoinJob {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MapSideJoinJob.class);


        job.setMapperClass(MapSideJoinMapper.class);
        //job.setReducerClass(Reducer.class);

        // job.setMapOutputKeyClass(NullWritable.class);
        // job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPaths(job,"/order/input");

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("/mapjoin/output"));

        // 如果不需要reduce阶段,那就需要把reduce的并行度设置为 ： 0
        job.setNumReduceTasks(0);

        // 指定一个数据文件，作为分布式缓存资源，那么也会随着job的其他资源一起放入将来的task运行的容器工作目录
        job.addCacheFile(new URI("hdfs://doitedu01:8020/order_user/input/user-info.txt"));


        System.exit(job.waitForCompletion(true)?0:1);
    }
}

package top.doe.mapreduce.demos.demo6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class HighJobMain {


    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);


        // TODO  别的参数要补上 !!!


        job.setPartitionerClass(MemberId_Partitioner.class);
        job.setGroupingComparatorClass(MemberId_GroupingComparator.class);


    }


}

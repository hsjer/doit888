package top.doe.mapreduce.demos.demo6;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MemberId_Partitioner extends HashPartitioner<OrderBean, NullWritable> {

    /**
     *  mapTask在输出map处理结果的时候，会调用 getPartition()
     *  来标注当前这条结果数据该分给哪个reduceTask
     */
    @Override
    public int getPartition(OrderBean key, NullWritable value, int numReduceTasks) {

        int memberId = key.getMember_id();

        return memberId % numReduceTasks;
    }
}

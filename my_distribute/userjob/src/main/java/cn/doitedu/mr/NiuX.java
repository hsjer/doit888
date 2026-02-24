package cn.doitedu.mr;

import top.doe.mr.client.Job;
import top.doe.mr.formats.FileInputFormat;

import java.io.IOException;

public class NiuX {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {

        Job job = new Job();
        job.setMapperClass(NiuXMapper.class);
        job.setInputFormatClass(FileInputFormat.class);
        job.setInputPath("hdfs://namenode:8020/aa/input");
        job.setOutputPath("hdfs://namenode:8020/aa/input");
        job.setMapTaskNumb(2);
        job.setMapTaskSplits(new String[]{"0:1,10","1:11,20"});
        job.setReduceTaskNumb(1);

        //
        job.submit("local");

    }
}

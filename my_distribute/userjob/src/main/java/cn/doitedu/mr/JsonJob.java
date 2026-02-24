package cn.doitedu.mr;

import top.doe.mr.client.Job;
import top.doe.mr.formats.FileInputFormat;

import java.io.IOException;

public class JsonJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {

        Job job = new Job();
        job.setMapperClass(JsonParseMapper.class);
        job.setInputFormatClass(FileInputFormat.class);
        job.setInputPath("hdfs://namenode:8020/aa/input");
        job.setOutputPath("hdfs://namenode:8020/aa/input");
//        job.setMapTaskNumb(5);
//        job.setMapTaskSplits(new String[]{"0:1,10","1:11,20","2:21,30","3:31,40","4:41,50"});

        job.setMapTaskNumb(1);
        job.setMapTaskSplits(new String[]{"0:1,10"});

        job.setReduceTaskNumb(1);

        job.setJarPath(args[0]);

        //
        job.submit(args[1]);

    }
}

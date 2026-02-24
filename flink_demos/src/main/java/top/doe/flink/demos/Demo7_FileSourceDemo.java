package top.doe.flink.demos;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo7_FileSourceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        TextLineInputFormat inputFormat = new TextLineInputFormat();

        FileSource<String> source =
                FileSource.forRecordStreamFormat(inputFormat, new Path("spark_data/wordcount/input"))
                        .build();


        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "myfile");
        System.out.println(stream.getParallelism());

        stream.print();

        env.execute();


    }
}

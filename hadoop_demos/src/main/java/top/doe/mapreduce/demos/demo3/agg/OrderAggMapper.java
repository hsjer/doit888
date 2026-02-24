package top.doe.mapreduce.demos.demo3.agg;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import top.doe.mapreduce.demos.demo3.join.OrderUser;

import java.io.IOException;

public class OrderAggMapper extends Mapper<NullWritable, OrderUser, Text,OrderUser> {

    @Override
    protected void map(NullWritable key, OrderUser value, Mapper<NullWritable, OrderUser, Text, OrderUser>.Context context) throws IOException, InterruptedException {

        String gender = value.getGender();
        context.write(new Text(gender),value);
    }
}

package top.doe.mapreduce.demos.demo3.join;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text, IntWritable,OrderUser> {

    String tableName;

    /**
     * mapTask的工作过程中，会调用一次 Mapper.setup()方法
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, IntWritable, OrderUser>.Context context) throws IOException, InterruptedException {

        // 从运行时上下文中获取当前MapTask所负责的任务片信息
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        Path path = inputSplit.getPath();
        tableName = path.getName().contains("order")? "order":"user";
    }

    @Override
    protected void map(LongWritable key, Text json, Mapper<LongWritable, Text, IntWritable, OrderUser>.Context context) throws IOException, InterruptedException {

        // {"order_id":"o1","member_id":1001,"amount":120.8,"receive_address":"beijing","date":"2024-08-1"}
        // {"id":1001,"member_level":2,"gender":"male"}

        OrderUser orderUser = JSON.parseObject(json.toString(), OrderUser.class);
        orderUser.setTableName(tableName);

        int id = (orderUser.getId() == null) ? orderUser.getMember_id() : orderUser.getId();

        context.write(new IntWritable(id), orderUser);

    }



     /**
     * mapTask的生命周期中，当自己负责的任务片数据处理完成时，会调用一次用户Mapper.cleanup()方法
     */
    @Override
    protected void cleanup(Mapper<LongWritable, Text, IntWritable, OrderUser>.Context context) throws IOException, InterruptedException {

    }
}

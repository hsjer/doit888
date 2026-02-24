package top.doe.mapreduce.demos.demo7;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TwoPhaseMapperTwo extends Mapper<LongWritable, Text, Text, OrderAggBean> {

    @Override
    protected void map(LongWritable key, Text json, Mapper<LongWritable, Text, Text, OrderAggBean>.Context context) throws IOException, InterruptedException {

        // {"province":"安徽省","order_count":2,"order_amount":550.0}

        OrderAggBean bean = JSON.parseObject(json.toString(), OrderAggBean.class);
        context.write(new Text(bean.getProvince()),bean);

    }
}

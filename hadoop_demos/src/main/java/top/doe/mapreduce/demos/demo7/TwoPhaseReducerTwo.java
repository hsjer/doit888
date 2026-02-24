package top.doe.mapreduce.demos.demo7;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoPhaseReducerTwo extends Reducer<Text, OrderAggBean, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<OrderAggBean> values, Reducer<Text, OrderAggBean, NullWritable, Text>.Context context) throws IOException, InterruptedException {

        double sum = 0;
        int cnt = 0;

        for (OrderAggBean bean : values) {

            cnt += bean.getOrder_count();
            sum += bean.getOrder_amount();
        }

        OrderAggBean bean = new OrderAggBean(key.toString(), sum, cnt);

        context.write(NullWritable.get(),new Text(JSON.toJSONString(bean)));
    }
}

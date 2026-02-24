package top.doe.mapreduce.demos.demo2;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

// 本例子中，输出的结果不需要key和value两个东西，只需要一个即可
// 所以，我们把结果放到key上输出， value 设置为 NullWritable（占位）
public class OrderTopnReducer extends Reducer<IntWritable, Order, Text, NullWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Order> values, Reducer<IntWritable, Order, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        // 收到的是：相同用户id  的一组订单
        // 输出： 这个用户的 金额最高的3笔订单
        ArrayList<Order> orderList = new ArrayList<>();

        // reduceTask所提供的这个迭代器
        //   首先，迭代器中会利用反射，得到一个空的 order对象
        //   每次迭代，其实是从本地工作目录的数据文件中读取字节数组，然后调用 上面的那个 order对象.readFields(in)
        //   然后再把这个对象返回给调用者
        for (Order value : values) {
            Order o = new Order(value.getOrder_id(), value.getMember_id(), value.getAmount(), value.getReceive_address(), value.getDate());
            orderList.add(o);
        }

        // 对list排序
        Collections.sort(orderList);

        // 输出前3
        for (int i = 0; i < Math.min(3, orderList.size()); i++) {
            Order order = orderList.get(i);
            String jsonString = JSON.toJSONString(order);
            context.write(new Text(jsonString), NullWritable.get());
        }

    }
}

package top.doe.mapreduce.demos.demo3.join;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class JoinReducer extends Reducer<IntWritable, OrderUser, NullWritable, OrderUser> {


    @Override
    protected void reduce(IntWritable key, Iterable<OrderUser> values, Reducer<IntWritable, OrderUser, NullWritable, OrderUser>.Context context) throws IOException, InterruptedException {

        /**
         * {"order_id":"o1","member_id":1001,"amount":120.8,"receive_address":"beijing","date":"2024-08-1"}
         * {"id":1001,"member_level":2,"gender":"male"}
         * {"order_id":"o5","member_id":1001,"amount":180.8,"receive_address":"beijing","date":"2024-08-2"}
         */

        ArrayList<OrderUser> orderList = new ArrayList<>();
        OrderUser user = null;


        // 迭代数据组
        // 把订单数据  和  用户数据  分离
        for (OrderUser value : values) {
            if (value.getTableName().equals("order")) {
                orderList.add(new OrderUser(value.getTableName(), value.getOrder_id(), value.getMember_id(), value.getAmount(), value.getReceive_address(), value.getDate(), value.getId(), value.getMember_level(), value.getGender()));
            } else {
                user = new OrderUser(value.getTableName(), value.getOrder_id(), value.getMember_id(), value.getAmount(), value.getReceive_address(), value.getDate(), value.getId(), value.getMember_level(), value.getGender());
            }


            return;
        }

        // 拼接
        if (user != null) {
            for (OrderUser joined : orderList) {

                // 为订单bean，填充user字段
                joined.setId(user.getId());
                joined.setMember_level(user.getMember_level());
                joined.setGender(user.getGender());

                joined.setTableName("joined");


                // 输出
                context.write(NullWritable.get(),joined);

            }
        }



    }
}

package top.doe.flink.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/10/10
 * @Desc: 学大数据，上多易教育
 *
 *  从控制台向nc服务输入数据：
 *     {"order_id":1,"order_amt":38.8,"order_type":"团购"}
 *     {"order_id":2,"order_amt":38.2,"order_type":"普通"}
 *     {"order_id":3,"order_amt":40.0,"order_type":"普通"}
 *     {"order_id":4,"order_amt":25.8,"order_type":"秒杀"}
 *     {"order_id":5,"order_amt":52.4,"order_type":"团购"}
 *     {"order_id":6,"order_amt":24.0,"order_type":"秒杀"}
 *
 *  用 flink实时统计：当前的每种类型的订单总金额
 *
 **/
public class Demo2_SocketOrderAmt {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9898);

        // 解析json
        SingleOutputStreamOperator<Order> orderStream = stream.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String json) throws Exception {
                return JSON.parseObject(json, Order.class);
            }
        });


        // keyBy分组
        KeyedStream<Order, String> keyedStream = orderStream.keyBy(od -> od.order_type);


        // 聚合sum (注意：sum是一个非生产级别的算子，很死，很怪：输入的类型 和 聚合的结果类型是一样的；
        // 每次变化的是目标聚合字段，而别的字段会使用组中的第一条的值
        SingleOutputStreamOperator<Order> resultStream = keyedStream.sum("order_amt");
        //resultStream.print();

        // max算子：输入的类型 和 聚合的结果类型是一致；
        // 每次变化的是目标聚合字段，而别的字段会使用组中的第一条的值
        /**
         * select
         *    order_id,
         *    receive_address,
         *    order_type,
         *    max(order_amt) as max_amt
         * from t
         * group by order_type
         */
        SingleOutputStreamOperator<Order> maxResult = keyedStream.max("order_amt");
        //maxResult.print();

        // max算子：输入的类型 和 聚合的结果类型是一致；
        // 只要最大值发生变化，那么结果就是最大值的这条数据
        /**
         * with tmp as (
         *     select
         *        order_id,
         *        receive_address,
         *        order_type,
         *        order_amt,
         *        row_number() over(partition by order_type order by order_amt desc ) as rn
         *     from t
         * )
         * select order_id,receive_address,order_type,order_amt from tmp where rn=1
         */
        SingleOutputStreamOperator<Order> maxByResult = keyedStream.maxBy("order_amt");
        maxByResult.print();


        SingleOutputStreamOperator<Order> minResult = keyedStream.min("order_amt");
        SingleOutputStreamOperator<Order> minByResult = keyedStream.minBy("order_amt");


        // 提交job
        env.execute();


    }



    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private int order_id;
        private double order_amt;
        private String order_type;

    }


}

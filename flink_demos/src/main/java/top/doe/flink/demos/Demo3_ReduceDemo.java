package top.doe.flink.demos;

import com.alibaba.fastjson.JSON;
import lombok.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
 * <p>
 * 从控制台向nc服务输入数据：
 * {"order_id":1,"order_amt":38.8,"order_type":"团购"}
 * {"order_id":2,"order_amt":38.2,"order_type":"普通"}
 * {"order_id":3,"order_amt":40.0,"order_type":"普通"}
 * {"order_id":4,"order_amt":25.8,"order_type":"秒杀"}
 * {"order_id":5,"order_amt":52.4,"order_type":"团购"}
 * {"order_id":6,"order_amt":24.0,"order_type":"秒杀"}
 * <p>
 * 用 flink实时统计：当前的每种类型的订单总金额
 **/
public class Demo3_ReduceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.用source算子加载数据
        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 7878);

        // 2.解析json
        SingleOutputStreamOperator<Order> orderStream = stream.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String value) throws Exception {
                return JSON.parseObject(value, Order.class);
            }
        });

        /* *
        // keyBy分组： 按订单类型
        KeyedStream<Order, String> keyedStream = orderStream.keyBy(od -> od.order_type);

        // 在 KeyedStream 上调用简单聚合算子：reduce
        // 各类订单的 平均订单额，最大订单金额，最小订单金额
        // 如下直接对order数据进行reduce，行不通，因为reduce的function的输入数据类型和累加器类型和结果类型都一致
        keyedStream.reduce(new ReduceFunction<Order>() {
            @Override
            public Order reduce(Order od1, Order od2) throws Exception {
                return null;
            }
        });
        */

        // 3.把输入数据order，转换成 符合需求的 累加器结构
        SingleOutputStreamOperator<OrderAgg> aggBeanStream = orderStream.map(new MapFunction<Order, OrderAgg>() {
            @Override
            public OrderAgg map(Order od) throws Exception {
                OrderAgg orderAgg = new OrderAgg();

                orderAgg.setOrder_type(od.order_type);
                orderAgg.setOrder_cnt(1);
                orderAgg.setSum(od.order_amt);
                orderAgg.setMinAmt(od.order_amt);
                orderAgg.setMaxAmt(od.order_amt);

                return orderAgg;
            }
        });

        // keyBy分组
        KeyedStream<OrderAgg, String> keyedStream = aggBeanStream.keyBy(agg -> agg.order_type);


        // 调用 reduce算子 聚合
        SingleOutputStreamOperator<OrderAgg> resultStream = keyedStream.reduce(new ReduceFunction<OrderAgg>() {
            @Override
            public OrderAgg reduce(OrderAgg agg, OrderAgg newData) throws Exception {

                // 订单数递增
                agg.order_cnt += newData.order_cnt;

                // 总额递增
                agg.sum += newData.sum;

                // 最大订单额更新
                agg.maxAmt = Math.max(newData.maxAmt, agg.maxAmt);

                // 最小订单额更新
                agg.minAmt = Math.min(agg.minAmt, newData.minAmt);

                return agg;
            }
        });


        // 调用sink算子输出结果
        resultStream.print();

        // 触发job执行
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


    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderAgg {
        private String order_type;
        private double sum;  // 总金额
        private int order_cnt;  // 总订单数
        private double maxAmt;
        private double minAmt;

        private Double avgAmt;

        public String getOrder_type() {
            return order_type;
        }

        public void setOrder_type(String order_type) {
            this.order_type = order_type;
        }

        public double getSum() {
            return sum;
        }

        public void setSum(double sum) {
            this.sum = sum;
        }

        public int getOrder_cnt() {
            return order_cnt;
        }

        public void setOrder_cnt(int order_cnt) {
            this.order_cnt = order_cnt;
        }

        public double getMaxAmt() {
            return maxAmt;
        }

        public void setMaxAmt(double maxAmt) {
            this.maxAmt = maxAmt;
        }

        public double getMinAmt() {
            return minAmt;
        }

        public void setMinAmt(double minAmt) {
            this.minAmt = minAmt;
        }


        public Double getAvgAmt(){
            return this.order_cnt == 0 ? this.sum : this.sum / this.order_cnt;
        }

        public void setAvgAmt(Double avgAmt) {
            this.avgAmt = avgAmt;
        }

        @Override
        public String toString() {
            return "OrderAgg{" +
                    "order_type='" + order_type + '\'' +
                    ", sum=" + sum +
                    ", order_cnt=" + order_cnt +
                    ", maxAmt=" + maxAmt +
                    ", minAmt=" + minAmt +
                    ", avgAmt=" + getAvgAmt() +
                    '}';
        }
    }


}

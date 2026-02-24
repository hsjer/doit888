package top.doe.flink.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.runtime.operators.window.assigners.SessionWindowAssigner;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Timer;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/10/16
 * @Desc: 学大数据，上多易教育
 *  下单后，15分没支付，则发送催支付信息

{"uid":3,"event_type":"submit_order","timestamp":1000,"properties":{"oid":1}}
{"uid":3,"event_type":"pay_order","timestamp":2000,"properties":{"oid":1}}
{"uid":3,"event_type":"add_cart","timestamp":3000,"properties":{"item_id":3,"num":2}}
{"uid":3,"event_type":"search","timestamp":4000,"properties":{"keyword":"咖啡"}}

 **/
public class Demo22_Flink_TimerDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");


        // {"uid":3,"event_type":"submit_order","timestamp":1000,"properties":{"oid":1}}
        // {"uid":3,"event_type":"pay_order","timestamp":2000,"properties":{"oid":1}}
        // {"uid":3,"event_type":"add_cart","timestamp":3000,"properties":{"item_id":3,"num":2}}
        // {"uid":3,"event_type":"search","timestamp":4000,"properties":{"keyword":"咖啡"}}
        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);
        SingleOutputStreamOperator<Event> eventStream = stream.map(s -> JSON.parseObject(s, Event.class));


        // 生成事件时间watermark
        SingleOutputStreamOperator<Event> wmStream = eventStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        );

        // 过滤，只留下订单相关事件： submit_order,  pay_order
        SingleOutputStreamOperator<Event> filtered = wmStream.filter(e -> e.event_type.equals("submit_order") || e.event_type.equals("pay_order"));



        // keyBy : 相同订单号的数据，要发到的相同的task并行度
        KeyedStream<Event, Integer> keyedStream = filtered.keyBy(e -> (int) e.properties.get("oid"));


        // 逻辑处理:
        SingleOutputStreamOperator<String> resultStream = keyedStream.process(new KeyedProcessFunction<Integer, Event, String>() {
            ValueState<Long> timerState;

            @Override
            public void open(Configuration parameters) throws Exception {

                timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer_state", Long.class));
            }

            @Override
            public void processElement(Event event, KeyedProcessFunction<Integer, Event, String>.Context ctx, Collector<String> out) throws Exception {

                if (event.event_type.equals("submit_order")) {
                    // 注册定时器 : 触发时间 =  此刻+15分
                    long timerTime = System.currentTimeMillis() + 1 * 30 * 1000;
                    ctx.timerService().registerProcessingTimeTimer(timerTime);

                    // 并且把定时器的唯一标识：定时时间，放入状态存储
                    timerState.update(timerTime);

                } else {
                    // 判断，是否要取消之前注册的定时器
                    if (timerState.value() != null && System.currentTimeMillis() - timerState.value() < 0) {
                        ctx.timerService().deleteProcessingTimeTimer(timerState.value());
                    }
                }
            }


            // 定时器触发时，系统回调该方法
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Integer, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

                Integer oid = ctx.getCurrentKey();
                out.collect("订单id:" + oid + ",请速支付");
            }
        });

        resultStream.print();


        env.execute();


    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static  class Event implements Serializable{
        private int uid;
        private String event_type;
        private long timestamp;
        private HashMap<String,Object> properties;
    }

}

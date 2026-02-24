import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/10/10
 * @Desc: 学大数据，上多易教育
 *   从socket服务器读取数据，做单词统计
 **/
public class UnorderToOrdered {

    public static void main(String[] args) throws Exception {

        // 创建编程入口（环境）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 加载数据源得到流
        DataStreamSource<String> stream = env.socketTextStream("doitedu", 9898);

        // 在流上调算子安排运算逻辑
        // 1. 切单词，生成元组对
        SingleOutputStreamOperator<Pair> tupleStream = stream.map(new MapFunction<String, Pair>() {
            @Override
            public Pair map(String s) throws Exception {
                // 切单词
                String[] split = s.split(",");

                return new Pair(split[0],Integer.parseInt(split[1]));
            }
        });


        // 2. 然后对上面的结果分组(相同key的数据会发给相同的下游task)
        KeyedStream<Pair, String> keyedStream = tupleStream.keyBy(tp -> tp.word);

        SingleOutputStreamOperator<Pair> orderQueue = keyedStream.process(new KeyedProcessFunction<String, Pair, Pair>() {
            ValueState<PriorityQueue<Pair>> orderQueueState;
            ValueState<Long> lastReceiveTime;
            ValueState<Long> timerTime;
            PriorityQueue<Pair> queue;
            long lastOutputTime = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                orderQueueState = getRuntimeContext().getState(new ValueStateDescriptor<PriorityQueue<Pair>>("order_queue", TypeInformation.of(new TypeHint<PriorityQueue<Pair>>() {
                })));


                lastReceiveTime = getRuntimeContext().getState(new ValueStateDescriptor<Long>("last_data_time", Long.class));
                timerTime = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer_time", Long.class));

            }

            @Override
            public void processElement(Pair tp, KeyedProcessFunction<String, Pair, Pair>.Context context, Collector<Pair> collector) throws Exception {
                long now = context.timerService().currentProcessingTime();

                if(lastReceiveTime.value()!=null &&  now - lastReceiveTime.value() <3000 && timerTime.value() != null) {
                    System.out.println("取消定时器... " + timerTime.value());
                    context.timerService().deleteProcessingTimeTimer(timerTime.value());
                }



                queue = orderQueueState.value();
                lastReceiveTime.update(now);

                if (queue == null) {
                    queue = new PriorityQueue<>(new Comparator<Pair>() {
                        @Override
                        public int compare(Pair o1, Pair o2) {
                            return Integer.compare(o1.time, o2.time);
                        }
                    });
                    orderQueueState.update(queue);
                }

                queue.offer(tp);

                if (queue.size() > 5 || (queue.size() > 0 &&  lastOutputTime!=0 && now - lastOutputTime > 3000)) {
                    collector.collect(queue.poll());
                    lastOutputTime = now;
                }


                context.timerService().registerProcessingTimeTimer(now+3000);
                timerTime.update(now+3000);

            }


            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Pair, Pair>.OnTimerContext ctx, Collector<Pair> out) throws Exception {

                if (queue.size()>0) {
                    out.collect(queue.poll());
                    System.out.println("定时器触发了.....");

                    ctx.timerService().registerProcessingTimeTimer(timestamp+3000);
                }

            }
        });


        SingleOutputStreamOperator<Pair> resultStream = orderQueue.forward().keyBy(p -> p.word)
                .process(new KeyedProcessFunction<String, Pair, Pair>() {
                    @Override
                    public void processElement(Pair pair, KeyedProcessFunction<String, Pair, Pair>.Context context, Collector<Pair> collector) throws Exception {
                        collector.collect(pair);
                    }
                });


        // 3. 在keyedStream调用聚合算子


        // 4.输出结果到控制台
        resultStream.print();


        // 触发执行
        env.execute("my-wordcount");


    }



    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Pair{
        private String word;
        private int time;
    }


}

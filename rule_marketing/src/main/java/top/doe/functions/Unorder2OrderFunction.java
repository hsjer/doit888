package top.doe.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.doe.bean.UserEvent;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.PriorityBlockingQueue;

public class Unorder2OrderFunction extends KeyedProcessFunction<Long, UserEvent, UserEvent> {
    ValueState<PriorityBlockingQueue<UserEvent>> queueState;
    ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {

        queueState = getRuntimeContext().getState(new ValueStateDescriptor<PriorityBlockingQueue<UserEvent>>("events", TypeInformation.of(new TypeHint<PriorityBlockingQueue<UserEvent>>() {
        })));


        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer_state", Long.class));


    }

    @Override
    public void processElement(UserEvent userEvent, KeyedProcessFunction<Long, UserEvent, UserEvent>.Context context, Collector<UserEvent> collector) throws Exception {

        /*if (timerState.value() != null) {
            context.timerService().deleteProcessingTimeTimer(timerState.value());
            timerState.clear();
        }*/

        //ConcurrentSkipListMap concurrentSkipListMap = new ConcurrentSkipListMap();


        PriorityBlockingQueue<UserEvent> queue = queueState.value();
        if (queue == null) {
            // PriorityQueue默认是：小顶堆
            queue = new PriorityBlockingQueue<>(100, new Comparator<UserEvent>() {
                @Override
                public int compare(UserEvent o1, UserEvent o2) {
                    return Long.compare(o1.getAction_time(), o2.getAction_time());
                }
            });
            queueState.update(queue);
        }

        queue.add(userEvent);


        if (queue.size() > 10) {
            UserEvent e = queue.poll();
            collector.collect(e);
        }

        long timerTime = System.currentTimeMillis() + 10000;
        context.timerService().registerProcessingTimeTimer(timerTime);
        timerState.update(timerTime);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, UserEvent, UserEvent>.OnTimerContext ctx, Collector<UserEvent> out) throws Exception {

        PriorityBlockingQueue<UserEvent> queue = queueState.value();
        if (queue != null) {
            UserEvent e ;
            while ((e = queue.poll()) != null) {
                out.collect(e);
            }
        }


    }
}

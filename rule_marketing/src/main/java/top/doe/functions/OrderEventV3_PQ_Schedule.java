package top.doe.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.doe.bean.UserEvent;

import java.util.concurrent.PriorityBlockingQueue;

@Slf4j
public class OrderEventV3_PQ_Schedule extends KeyedProcessFunction<Long, UserEvent, UserEvent> {
    ValueState<PriorityBlockingQueue<UserEvent>> treeState;
    ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {

        treeState = getRuntimeContext().getState(
                new ValueStateDescriptor<PriorityBlockingQueue<UserEvent>>("events", TypeInformation.of(new TypeHint<PriorityBlockingQueue<UserEvent>>() {
                })));

        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer_state", Long.class));
    }

    @Override
    public void processElement(UserEvent userEvent, KeyedProcessFunction<Long, UserEvent, UserEvent>.Context context, Collector<UserEvent> collector) throws Exception {

        if (userEvent.getEvent_id().equals("START")) {
            collector.collect(userEvent);

            return;
        }

        if (timerState.value() != null) {
            context.timerService().deleteProcessingTimeTimer(timerState.value());
        }

        PriorityBlockingQueue<UserEvent> queue = treeState.value();
        if (queue == null) {
            queue = new PriorityBlockingQueue<UserEvent>();
            treeState.update(queue);
        }

        queue.put(userEvent);

        if (queue.size() > 5) {
            UserEvent e = queue.poll();
            collector.collect(e);
        }


        long timerTime = System.currentTimeMillis() + 10000;
        context.timerService().registerProcessingTimeTimer(timerTime);
        timerState.update(timerTime);

    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, UserEvent, UserEvent>.OnTimerContext ctx, Collector<UserEvent> out) throws Exception {
        PriorityBlockingQueue<UserEvent> queue = treeState.value();


        if (queue != null) {
            UserEvent e;
            while ((e = queue.poll()) != null) {
                out.collect(e);
            }

            long timerTime = timestamp + 10000;
            ctx.timerService().registerProcessingTimeTimer(timerTime);
            timerState.update(timerTime);

        }
    }
}

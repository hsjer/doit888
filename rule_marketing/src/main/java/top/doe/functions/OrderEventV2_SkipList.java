package top.doe.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import top.doe.bean.UserEvent;
import top.doe.utils.EventTimeComparator;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class OrderEventV2_SkipList extends KeyedProcessFunction<Long, UserEvent, UserEvent> {
    ValueState<ConcurrentSkipListMap<UserEvent,String>> treeState;
    ValueState<Long> timerState;
    String v = "";

    @Override
    public void open(Configuration parameters) throws Exception {

        treeState = getRuntimeContext().getState(
                new ValueStateDescriptor<ConcurrentSkipListMap<UserEvent,String>>("events", TypeInformation.of(new TypeHint<ConcurrentSkipListMap<UserEvent,String>>() {})));

        timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer_state", Long.class));



    }

    @Override
    public void processElement(UserEvent userEvent, KeyedProcessFunction<Long, UserEvent, UserEvent>.Context context, Collector<UserEvent> collector) throws Exception {

        if(userEvent.getEvent_id().equals("START")){
            collector.collect(userEvent);
            return;
        }

        if (timerState.value() != null) {
            context.timerService().deleteProcessingTimeTimer(timerState.value());
        }

        ConcurrentSkipListMap<UserEvent, String> tree = treeState.value();
        if(tree==null){
            tree = new ConcurrentSkipListMap<>(new EventTimeComparator());
            treeState.update(tree);
        }

        tree.put(userEvent,v);
        treeState.update(tree);

        if(tree.size()>5){
            Map.Entry<UserEvent, String> firstEntry = tree.pollFirstEntry();
            collector.collect(firstEntry.getKey());
        }


        long timerTime = System.currentTimeMillis() + 1000;
        context.timerService().registerProcessingTimeTimer(timerTime);
        timerState.update(timerTime);

    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, UserEvent, UserEvent>.OnTimerContext ctx, Collector<UserEvent> out) throws Exception {
        ConcurrentSkipListMap<UserEvent, String> tree = treeState.value();


        if (tree != null) {
            Map.Entry<UserEvent, String> entry;
            while( (entry = tree.pollFirstEntry())!=null) {
                out.collect(entry.getKey());
            }

            long timerTime = timestamp + 1000;
            ctx.timerService().registerProcessingTimeTimer(timerTime);
            timerState.update(timerTime);
        }
    }
}

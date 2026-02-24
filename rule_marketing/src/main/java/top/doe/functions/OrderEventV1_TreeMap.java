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
import java.util.Map;
import java.util.TreeMap;

public class OrderEventV1_TreeMap extends KeyedProcessFunction<Long, UserEvent, UserEvent> {
    ValueState<TreeMap<UserEvent,String>> treeState;
    ValueState<Long> timerState;
    String v = "";

    @Override
    public void open(Configuration parameters) throws Exception {

        treeState = getRuntimeContext().getState(
                new ValueStateDescriptor<TreeMap<UserEvent,String>>("events", TypeInformation.of(new TypeHint<TreeMap<UserEvent,String>>() {})));

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
            //log.warn("删除了timer:{},当前key:{}",timerState.value(),context.getCurrentKey());
        }

        TreeMap<UserEvent, String> tree = treeState.value();
        if(tree==null){
            tree = new TreeMap<>(new Comparator<UserEvent>() {
                @Override
                public int compare(UserEvent o1, UserEvent o2) {
                    return Long.compare(o1.getAction_time(),o2.getAction_time());
                }
            });
            treeState.update(tree);
        }

        tree.put(userEvent,v);
        treeState.update(tree);

        if(tree.size()>5){
            Map.Entry<UserEvent, String> firstEntry = tree.pollFirstEntry();
            collector.collect(firstEntry.getKey());
            //log.warn("输出tree中事件: {}",firstEntry.getKey() );
        }


        long timerTime = System.currentTimeMillis() + 1000;
        context.timerService().registerProcessingTimeTimer(timerTime);
        timerState.update(timerTime);
        //log.warn("此刻是:{},注册了timer: {},当前key:{}",timerTime-1000,timerTime,context.getCurrentKey());

    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, UserEvent, UserEvent>.OnTimerContext ctx, Collector<UserEvent> out) throws Exception {
        //log.warn("timer触发了,当前key:{},当前时间:{}",ctx.getCurrentKey(),timestamp);
        TreeMap<UserEvent, String> tree = treeState.value();


        if (tree != null) {
            Map.Entry<UserEvent, String> entry;
            while( (entry = tree.pollFirstEntry())!=null) {
                out.collect(entry.getKey());
                //log.warn("timer触发,tree中输出数据,当前key:{},数据:{}",ctx.getCurrentKey(),entry.getKey());
            }

            long timerTime = timestamp + 1000;
            ctx.timerService().registerProcessingTimeTimer(timerTime);
            timerState.update(timerTime);
        }
    }
}

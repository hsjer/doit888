//package top.doe.functions;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.common.state.*;
//import org.apache.flink.api.common.time.Time;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.runtime.state.FunctionInitializationContext;
//import org.apache.flink.runtime.state.FunctionSnapshotContext;
//import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
//import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
//import org.apache.flink.util.Collector;
//import top.doe.bean.RuleMeta;
//import top.doe.bean.UserEvent;
//import top.doe.calculator_model.RuleCalculator;
//import top.doe.calculators.RuleMode2Calculator;
//
//import java.util.HashMap;
//import java.util.Map;
//
//
//@Slf4j
//public class RuleCalculateCore_BroadcastState_Function extends KeyedBroadcastProcessFunction<String, UserEvent, RuleMeta, String> {
//
//    // 规则运算机池
//    //final HashMap<Integer, RuleCalculator> calculatorPool = new HashMap<>();
//
//
//    // 用于缓存最近2分钟的用户行为明细
//    ListState<UserEvent> eventsBuffer;
//
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        // 创建listState的描述器，并开启ttl
//        ListStateDescriptor<UserEvent> desc = new ListStateDescriptor<>("event_buffer", UserEvent.class);
//        desc.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(2)).build());
//
//        // 获取listState
//        eventsBuffer = getRuntimeContext().getListState(desc);
//
//    }
//
//    @Override
//    public void processElement(UserEvent userEvent, KeyedBroadcastProcessFunction<String, UserEvent, RuleMeta, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
//
//        ReadOnlyBroadcastState<Integer, RuleCalculator> calculatorPool = readOnlyContext.getBroadcastState(new MapStateDescriptor<Integer, RuleCalculator>("calculator_pool", Integer.class, RuleCalculator.class));
//
//
//        // 将最新接收的用户行为数据，缓存到eventsBuffer中
//        eventsBuffer.add(userEvent);
//
//        // 遍历各规则的运算机，处理数据
//        for (Map.Entry<Integer, RuleCalculator> entry : calculatorPool.immutableEntries()) {
//            RuleCalculator calculator = entry.getValue();
//
//            // 判断该运算机是否是新运算机
//            if (calculator.getNewStatus()) {
//                // 如果是新运算机，则把缓存中的数据（包括最新的这一条）逐条交给运算机处理
//                for (UserEvent event : eventsBuffer.get()) {
//                    calculator.calculate(event, collector);
//                }
//                // 新运算机处理完这些数据后，就不再是新运算机了
//                calculator.setNewStatus(false);
//            } else {
//                // 否则，只把最新的这一条数据交给运算机处理
//                calculator.calculate(userEvent, collector);
//            }
//        }
//    }
//
//    @Override
//    public void processBroadcastElement(RuleMeta ruleMeta, KeyedBroadcastProcessFunction<String, UserEvent, RuleMeta, String>.Context context, Collector<String> collector) throws Exception {
//
//        BroadcastState<Integer, RuleCalculator> calculatorPool = context.getBroadcastState(new MapStateDescriptor<Integer, RuleCalculator>("calculator_pool", Integer.class, RuleCalculator.class));
//
//
//
//        /* *
//         *  规则的动态上线 或 动态更新
//         */
//        // 接收的元数据有如下操作类型： r/c 增， d/删除,  u/ 更新： 运算机状态下线
//        if ((ruleMeta.getOp().equals("r") || ruleMeta.getOp().equals("c") || (ruleMeta.getOp().equals("u")) && ruleMeta.getRule_status() == 1)) {
//
//            // 创建运算机实例对象
//            RuleCalculator ruleCalculator = new RuleMode2Calculator();
//            // 初始化运算机
//            ruleCalculator.initialize(getRuntimeContext(), ruleMeta);
//            // 放入运算机池
//            calculatorPool.put(ruleMeta.getId(), ruleCalculator);
//
//            log.warn("规则上线,规则id:{},目标人群:{}", ruleMeta.getId(), ruleMeta.getRule_crowd_bitmap().toString());
//        }
//
//
//        /* *
//         *  规则的动态下线
//         */
//        if (ruleMeta.getOp().equals("d") || ruleMeta.getRule_status() == 0) {
//            // 从运算机池移除该规则运算机对象
//            calculatorPool.remove(ruleMeta.getId());
//
//            log.warn("规则下线,规则id:{}", ruleMeta.getId());
//        }
//    }
//}

package top.doe.functions;

import groovy.lang.GroovyClassLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import top.doe.bean.RuleMeta;
import top.doe.bean.UserEvent;
import top.doe.calculator_model.RuleCalculator;
import top.doe.threads.CalculatorRunner;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Slf4j
public class RuleCalculateCoreFunction extends KeyedBroadcastProcessFunction<Long, UserEvent, RuleMeta, String> implements CheckpointedFunction {


    // 这个hashmap必须是线程安全的，因为  processElement() [读] 和  processBroadcastElement()[读、写]  会对它进行并行访问
    final ConcurrentHashMap<Integer, CalculatorRunner> calculatorRunnerPool = new ConcurrentHashMap<>();

    // 规则元数据存储
    final HashMap<Integer, RuleMeta> ruleMetaHashMap = new HashMap<>();

    // 用于缓存最近2分钟的用户行为明细
    ListState<UserEvent> eventsBuffer;

    ListState<RuleMeta> ruleMetaListState;

    // 运行规则计算任务的线程池
    ExecutorService executorService;

    //SimpleCompiler simpleCompiler;
    GroovyClassLoader groovyClassLoader;


    final HashMap<String, Class<?>> CLASS_CACHE = new HashMap<>();

    long start = 0;
    long end = 0;

    int dataCount = 0;

    // 同步锁
    private final String lock = "lock";


    @Override
    public void open(Configuration parameters) throws Exception {


        // 创建listState的描述器，并开启ttl
        ListStateDescriptor<UserEvent> desc = new ListStateDescriptor<>("event_buffer", UserEvent.class);
        desc.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(2)).build());

        // 获取listState
        eventsBuffer = getRuntimeContext().getListState(desc);

        // 运行规则计算任务的线程池
        executorService = Executors.newCachedThreadPool();

        // 创建一个janino的编译器
        //simpleCompiler = new SimpleCompiler();
        //simpleCompiler.setParentClassLoader(this.getClass().getClassLoader());

        groovyClassLoader = new GroovyClassLoader();

    }

    @Override
    public void processElement(UserEvent userEvent, KeyedBroadcastProcessFunction<Long, UserEvent, RuleMeta, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {


        if (userEvent.getEvent_id().equals("ERR") && RandomUtils.nextInt(1, 10) % 3 == 0) {
            log.warn("收到一个ERR,且马上就要抛出异常......");
            throw new RuntimeException("ERR..........................");
        }

        dataCount++;

        if (userEvent.getEvent_id().equals("START")) {
            start = System.currentTimeMillis();
            log.warn("FLAG:第一条数据处理开始,action_time:{}, 当前时间:{}", userEvent.getAction_time(), start);
        }

        // 将最新接收的用户行为数据，缓存到eventsBuffer中
        eventsBuffer.add(userEvent);

        CountDownLatch latch = new CountDownLatch(calculatorRunnerPool.size());

        // 遍历各规则的运算机，处理数据
        synchronized (lock) {
            for (Map.Entry<Integer, CalculatorRunner> entry : calculatorRunnerPool.entrySet()) {
                CalculatorRunner runner = entry.getValue();
                runner.setCollector(collector);
                runner.setLatch(latch);

                // 判断该运算机是否是新运算机
                if (runner.getCalculator().getNewStatus()) {
                    // 如果是新运算机，则把缓存中的数据（包括最新的这一条）逐条交给运算机处理
                    runner.setUserEvents(eventsBuffer.get());
                    executorService.submit(runner);

                } else {
                    // 否则，只把最新的这一条数据交给运算机处理
                    runner.setUserEvents(Collections.singletonList(userEvent));
                    executorService.submit(runner);
                }
            }
        }
        // 等待所有异步任务全部完成
        latch.await();
        if (userEvent.getEvent_id().equals("END")) {
            end = System.currentTimeMillis();
            log.warn("FLAG:最后一条数据处理完成,action_time:{},总数据：{} 起始时间:{},当前时间:{},总耗时:{}", userEvent.getAction_time(), dataCount, start, end, (end - start) / 1000);
        }

    }

    @Override
    public void processBroadcastElement(RuleMeta ruleMeta, KeyedBroadcastProcessFunction<Long, UserEvent, RuleMeta, String>.Context context, Collector<String> collector) throws Exception {
        /* *
         *  规则的动态上线 或 动态更新
         */
        // 接收的元数据有如下操作类型： r/c 增， d/删除,  u/ 更新： 运算机状态下线
        if (((ruleMeta.getOp().equals("r") || ruleMeta.getOp().equals("c") || ruleMeta.getOp().equals("u")) && ruleMeta.getRule_status() == 1)) {

            RuleCalculator ruleCalculator;

            //// 先从缓冲中获取已经编译好的class
            Class<?> cacheClass = CLASS_CACHE.get(ruleMeta.getRule_model_classname());
            if (cacheClass == null) {
                // 编译源代码，加载类
                cacheClass = groovyClassLoader.parseClass(ruleMeta.getRule_model_code());
                CLASS_CACHE.put(ruleMeta.getRule_model_classname(), cacheClass);
            }
            // 反射实例化
            ruleCalculator = (RuleCalculator) cacheClass.newInstance();

            //ruleCalculator = new RuleModel3Calculator();


            // 初始化运算机
            ruleCalculator.initialize(getRuntimeContext(), ruleMeta, null);

            // 把运算机对象包装成runner
            CalculatorRunner runner = new CalculatorRunner(ruleCalculator);

            // 放入运算机runner池
            calculatorRunnerPool.put(ruleMeta.getId(), runner);

            // 把规则元数据放入元数据hashmap
            ruleMetaHashMap.put(ruleMeta.getId(), ruleMeta);

            log.warn("规则上线,规则id:{},模型id:{},目标人群人数:{}", ruleMeta.getId(), ruleMeta.getRule_model_id(), ruleMeta.getRule_crowd_bitmap().getIntCardinality());
        }


        /* *
         *  规则的动态下线
         */
        if (ruleMeta.getOp().equals("d") || ruleMeta.getRule_status() == 0) {
            synchronized (lock) {
                // 从运算机池移除该规则运算机对象
                CalculatorRunner runner = calculatorRunnerPool.remove(ruleMeta.getId());

                // 从规则元数据集合中，删除本规则的元数据
                ruleMetaHashMap.remove(ruleMeta.getId());

                // 清理该规则的运算机的状态
                StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
                runner.getCalculator().destroy(runtimeContext.getOperator());

            }
            log.warn("规则下线,规则id:{}", ruleMeta.getId());
        }
    }


    /**
     * checkpoint时会调用的方法
     *
     * @param functionSnapshotContext
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // 先把当前系统中的元数据转成一个list
        ArrayList<RuleMeta> metaList = new ArrayList<>(ruleMetaHashMap.values());

        // 然后把当前的所有元数据，放入list状态
        ruleMetaListState.update(metaList);

    }

    /**
     * 恢复状态时会调用的方法
     *
     * @param functionInitializationContext
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

        if (groovyClassLoader == null) {
            groovyClassLoader = new GroovyClassLoader();
        }

        // 获取 用于存储  规则元数据的  UnionList 算子状态
        ruleMetaListState =
                functionInitializationContext.getOperatorStateStore()
                        .getUnionListState(new ListStateDescriptor<RuleMeta>("calculators", RuleMeta.class));

        // 遍历状态中的每一个规则的元数据，构造规则的运算机，并初始化，并放入运算机池
        for (RuleMeta ruleMeta : ruleMetaListState.get()) {

            // 把恢复出来的元数据，放入系统当前的元数据池
            ruleMetaHashMap.put(ruleMeta.getId(), ruleMeta);


            // 创建运算机实例对象
            RuleCalculator ruleCalculator;

            // 利用janino动态编译运算机源代码，创建运算机实例对象
            //simpleCompiler.cook(ruleMeta.getRule_model_code());
            //Class<?> aClass = simpleCompiler.getClassLoader().loadClass(ruleMeta.getRule_model_classname());
            //ruleCalculator = (RuleCalculator) aClass.newInstance();

            // 先从缓冲中获取已经编译好的class
            Class<?> cacheClass = CLASS_CACHE.get(ruleMeta.getRule_model_classname());
            if (cacheClass == null) {
                cacheClass = groovyClassLoader.parseClass(ruleMeta.getRule_model_code());
                CLASS_CACHE.put(ruleMeta.getRule_model_classname(), cacheClass);
            }

            ruleCalculator = (RuleCalculator) cacheClass.newInstance();
            // 初始化运算机
            KeyedStateStore keyedStateStore = functionInitializationContext.getKeyedStateStore();
            ruleCalculator.initialize(null, ruleMeta, keyedStateStore);


            // 把运算机对象，包装成runner对象
            CalculatorRunner runner = new CalculatorRunner(ruleCalculator);


            // 放入运算机runner池
            calculatorRunnerPool.put(ruleMeta.getId(), runner);


            log.warn("规则运算机恢复,规则id:{},所属模型:{},目标人群人数:{}", ruleMeta.getId(), ruleMeta.getRule_model_id(), ruleMeta.getRule_crowd_bitmap().getIntCardinality());
        }
    }

}

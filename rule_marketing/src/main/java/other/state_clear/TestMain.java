package other.state_clear;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;

public class TestMain {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        env.setParallelism(1);


        DataStreamSource<String> events = env.socketTextStream("doitedu01", 9991);

        DataStreamSource<String> meta = env.socketTextStream("doitedu01", 9992);
        BroadcastStream<String> meta_bc = meta.broadcast(new MapStateDescriptor<String, String>("bc_state", String.class, String.class));


        events.keyBy(s -> s)
                .connect(meta_bc)
                .process(new KeyedBroadcastProcessFunction<String, String, String, String>() {
                    ListState<String> st;
                    Object lock ;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        lock = new Object();
                        st = getRuntimeContext().getListState(new ListStateDescriptor<String>("st", String.class));
                    }

                    @Override
                    public void processElement(String s, KeyedBroadcastProcessFunction<String, String, String, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {

                        synchronized (lock) {
                            st.add(s);

                            StringBuilder sb = new StringBuilder();
                            for (String v : st.get()) {
                                sb.append(v);
                            }

                            collector.collect(sb.toString());
                        }
                    }

                    @Override
                    public void processBroadcastElement(String s, KeyedBroadcastProcessFunction<String, String, String, String>.Context context, Collector<String> collector) throws Exception {

                        String[] ids = {"a", "b", "c"};
                        if (s.equals("X")) {

                            synchronized (lock) {
                                StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
                                AbstractStreamOperator<?> operator = runtimeContext.getOperator();

                                Object currentKey = operator.getCurrentKey();

                                // 清理状态
                                for (String id : ids) {
                                    operator.setCurrentKey(id);
                                    st.clear();
                                }

                                // 移除运算机

                                // 恢复此前的key
                                operator.setCurrentKey(currentKey);
                            }
                        }
                    }
                }).print();

        env.execute();


    }
}

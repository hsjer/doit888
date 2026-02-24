package top.doe.flink.demos;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class Demo15_Flink_OperatorState {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");


        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);

        // 没有key上下文，如果要用状态，只能用operator state
        stream.shuffle().map(new MyMapFunction()).print();

        env.execute();
    }

    public static class MyMapFunction implements MapFunction<String, String>, CheckpointedFunction{
        List<String> myLst = new ArrayList<>();

        ListState<String> listState;

        @Override
        public String map(String value) throws Exception {

            myLst.add(value);

            StringBuilder sb = new StringBuilder();
            for (String s : myLst) {
                sb.append(s);
            }

            return sb.toString();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            // 最佳实践： 从自己的内存集合中，把最新数据，更新到状态容器中
            listState.update(myLst);

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            // 从context中获取系统恢复出来的状态数据
            listState = context.getOperatorStateStore().getListState(new ListStateDescriptor<String>("list-state", String.class));

            // 最佳实践：从listState中遍历出数据，放入自己的内存集合对象
            for (String s : listState.get()) {
                myLst.add(s);
            }

        }
    }


}

package top.doe.flink.demos;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo16_Flink_KeyedState {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");


        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);


        stream.keyBy(s->s).map(new RichMapFunction<String, String>() {

            ValueState<Integer> cntState;
            ListState<String> listState;


            @Override
            public void open(Configuration parameters) throws Exception {

                RuntimeContext runtimeContext = getRuntimeContext();

                cntState = runtimeContext.getState(new ValueStateDescriptor<Integer>("cnt", Integer.class));

                // 不能在这里读写状态中的数据，因为此刻没有key上下文
                cntState.update(0);

                listState = runtimeContext.getListState(new ListStateDescriptor<String>("seq", String.class));

            }

            @Override
            public String map(String value) throws Exception {

                cntState.update(cntState.value() ==  null ? 1 : cntState.value()+1);

                listState.add(value);

                System.out.println("此刻，cntState: " + cntState.value());

                System.out.print("此刻，listState: ");

                for (String s : listState.get()) {
                    System.out.print(s);
                }
                System.out.println("-------------------------");

                return value;
            }

        }).print();

        env.execute();




    }
}

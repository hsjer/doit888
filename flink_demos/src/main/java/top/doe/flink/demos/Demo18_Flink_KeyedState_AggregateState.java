package top.doe.flink.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo18_Flink_KeyedState_AggregateState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");


        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6);

        KeyedStream<Integer, String> keyedStream = stream.keyBy(e -> "ok");


        keyedStream.map(new RichMapFunction<Integer, Double>() {
            AggregatingState<Integer, Double> aggState;

            @Override
            public void open(Configuration parameters) throws Exception {

               aggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Pair, Double>("agg", new AggregateFunction<Integer, Pair, Double>() {
                    @Override
                    public Pair createAccumulator() {
                        return new Pair();
                    }

                    @Override
                    public Pair add(Integer value, Pair accumulator) {

                        accumulator.cnt++;
                        accumulator.sum += value;

                        return accumulator;
                    }

                    @Override
                    public Double getResult(Pair accumulator) {

                        return accumulator.sum / accumulator.cnt;
                    }

                    @Override
                    public Pair merge(Pair a, Pair b) {
                        a.cnt += b.cnt;
                        a.sum += b.sum;
                        return a;
                    }
                }, Pair.class));


            }

            @Override
            public Double map(Integer value) throws Exception {

                aggState.add(value);

                return aggState.get();
            }
        }).print();


        env.execute();


    }


    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class Pair {
        private int cnt;
        private double sum;
    }
}

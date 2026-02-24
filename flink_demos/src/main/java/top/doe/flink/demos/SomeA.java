package top.doe.flink.demos;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SomeA {
    int a;
    int b;
    public SomeA(int a,int b){
        this.a =a;
        this.b = b;
    }

    public void doSomeA(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9099);

        stream.map(s->s).filter(s->s.startsWith("a")).print();
        env.execute();
    }
}

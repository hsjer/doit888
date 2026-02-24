package top.doe.functions;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class BenchSink implements SinkFunction<String> {

    @Override
    public void invoke(String value, Context context) throws Exception {
        if(RandomUtils.nextInt(1,19) % 5 == 0){
            System.out.println(value);
        }
    }
}

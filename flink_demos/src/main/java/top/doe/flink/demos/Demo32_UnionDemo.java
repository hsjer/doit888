package top.doe.flink.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;

public class Demo32_UnionDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // csv  
        // 1,zs,male
        DataStreamSource<String> stream1 = env.socketTextStream("doitedu01", 9999);

        SingleOutputStreamOperator<Person> perStream1 = stream1.map(s -> {
            String[] split = s.split(",");
            return new Person(Integer.parseInt(split[0]), split[1], split[2].equals("male") ? 1 : 0);
        });


        //{"uid":1,"name":zs,"gender":1}
        DataStreamSource<String> stream2 = env.socketTextStream("doitedu01", 9998);
        SingleOutputStreamOperator<Person> perStream2 = stream2.map(json -> JSON.parseObject(json, Person.class));


        // 上述两个数据源过来的数据，在业务逻辑上可能属于相同的业务范畴，可以把他们合在一起处理
        DataStream<Person> unioned = perStream1.union(perStream2);

        SingleOutputStreamOperator<String> mapped = unioned.map(new MapFunction<Person, String>() {
            @Override
            public String map(Person value) throws Exception {
                return JSON.toJSONString(value);
            }
        });


    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Person {
        private int uid;
        private String name;
        private int gender;
    }


}

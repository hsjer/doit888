package top.doe.flink.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;

public class Demo32_ConnectDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // csv  
        // 1,zs,male
        DataStreamSource<String> stream1 = env.socketTextStream("doitedu01", 9999);

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = stream1.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String s) throws Exception {
                String[] split = s.split(",");
                return Tuple3.of(split[0], split[1], split[2]);
            }
        });


        //{"uid":1,"name":zs,"gender":1}
        DataStreamSource<String> stream2 = env.socketTextStream("doitedu01", 9998);


        // 连接两个流

        SingleOutputStreamOperator<Person> stream = tpStream.connect(stream2)
                .map(new RichCoMapFunction<Tuple3<String, String, String>, String, Person>() {

                    // 如果需要在两个流的数据处理过程中，使用到一些共享信息，则可以在这里定义成员变量，或者用状态

                    @Override
                    public void open(Configuration parameters) throws Exception {

                    }

                    @Override
                    public Person map1(Tuple3<String, String, String> tp3) throws Exception {
                        return new Person(Integer.parseInt(tp3.f0), tp3.f1, tp3.f2.equals("male") ? 1 : 0);
                    }

                    @Override
                    public Person map2(String json) throws Exception {
                        return JSON.parseObject(json, Person.class);
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

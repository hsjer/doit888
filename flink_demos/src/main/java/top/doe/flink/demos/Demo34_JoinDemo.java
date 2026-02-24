package top.doe.flink.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Demo34_JoinDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // csv
        // {"id":1,"name":zs,"gender":1}
        DataStreamSource<String> stream1 = env.socketTextStream("doitedu01", 9999);
        SingleOutputStreamOperator<Person> personStream = stream1.map(json -> JSON.parseObject(json, Person.class));


        //{"uid":1,"score":80}
        DataStreamSource<String> stream2 = env.socketTextStream("doitedu01", 9998);
        SingleOutputStreamOperator<Score> scoreStream = stream2.map(s -> JSON.parseObject(s, Score.class));


        DataStream<String> result = personStream.join(scoreStream)
                .where(p -> p.id)
                .equalTo(s -> s.uid)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .apply(new JoinFunction<Person, Score, String>() {
                    // 一次窗口中，该方法会调用多次；只要有左右两边能连上的数据，每一对都会调用一次，让你输出结果
                    @Override
                    public String join(Person first, Score second) throws Exception {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("student_id", first.id);
                        jsonObject.put("student_name", first.name);
                        jsonObject.put("student_gender", first.gender);
                        jsonObject.put("student_score", second.score);

                        return jsonObject.toJSONString();
                    }
                });

        result.print();

        env.execute();

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Person {
        private int id;
        private String name;
        private int gender;
    }



    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Score {
        private int uid;
        private int score;
    }


}

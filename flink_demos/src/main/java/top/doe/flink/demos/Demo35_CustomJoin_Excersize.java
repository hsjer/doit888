package top.doe.flink.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class Demo35_CustomJoin_Excersize {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // csv
        // {"id":1,"name":zs,"gender":1}
        DataStreamSource<String> stream1 = env.socketTextStream("doitedu01", 9999);
        SingleOutputStreamOperator<Person> personStream = stream1.broadcast().map(json -> JSON.parseObject(json, Person.class));


        //{"uid":1,"score":80}
        DataStreamSource<String> stream2 = env.socketTextStream("doitedu01", 9998);
        SingleOutputStreamOperator<Score> scoreStream = stream2.map(s -> JSON.parseObject(s, Score.class));


        KeyedStream<Person, Integer> keyed1 = personStream.keyBy(p -> p.id);
        KeyedStream<Score, Integer> keyed2 = scoreStream.keyBy(s -> s.uid);

        // 连接两个流进行处理
        SingleOutputStreamOperator<String> resultStream = keyed1.connect(keyed2)
                .process(new KeyedCoProcessFunction<Integer, Person, Score, String>() {

                    ListState<Person> leftState;
                    ListState<Score> rightState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 获取状态容器
                        ListStateDescriptor<Person> desc1 = new ListStateDescriptor<>("left-state", Person.class);
                        desc1.enableTimeToLive(new StateTtlConfig.Builder(Time.minutes(30)).updateTtlOnReadAndWrite().build());
                        leftState = getRuntimeContext().getListState(desc1);


                        ListStateDescriptor<Score> desc2 = new ListStateDescriptor<>("right-state", Score.class);
                        desc2.enableTimeToLive(new StateTtlConfig.Builder(Time.minutes(30)).updateTtlOnReadAndWrite().build());
                        rightState = getRuntimeContext().getListState(desc2);

                    }

                    @Override
                    public void processElement1(Person person, KeyedCoProcessFunction<Integer, Person, Score, String>.Context ctx, Collector<String> out) throws Exception {

                        boolean find = false;
                        // 先到右表状态去匹配
                        for (Score score : rightState.get()) {
                            int uid = score.uid;
                            if (person.id == uid) {
                                person.setScore(score.score);
                                person.setRowType("+I");
                                out.collect(person.toString());

                                find = true;
                            }
                        }

                        // 如果没有匹配上
                        if(!find){
                            // 则输出一个LEFT结果
                            person.setRowType("+I");
                            out.collect(person.toString());
                        }


                        // 将数据存入左流状态
                        leftState.add(person);

                    }

                    @Override
                    public void processElement2(Score score, KeyedCoProcessFunction<Integer, Person, Score, String>.Context ctx, Collector<String> out) throws Exception {

                        // 处理右流的数据
                        for (Person person : leftState.get()) {
                            if (person.id == score.uid) {
                                // 如果遍历到的坐标数据score字段为空，说明此前它已经输出过一条 “不正确”的结果
                                if (person.score == null) {
                                    // 输出 -U/+U
                                    person.setRowType("-U");
                                    out.collect(person.toString());

                                    person.setRowType("+U");
                                    person.setScore(score.score);
                                    out.collect(person.toString());
                                } else {

                                    person.setRowType("+I");
                                    person.setScore(score.score);
                                    out.collect(person.toString());
                                }
                            }
                        }

                        // 数据存入状态
                        rightState.add(score);

                    }
                });


        //
        resultStream.print();


        env.execute();

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Person {
        private String rowType;
        private int id;
        private String name;
        private int gender;
        private Integer score;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Score {
        private int uid;
        private int score;
    }

}

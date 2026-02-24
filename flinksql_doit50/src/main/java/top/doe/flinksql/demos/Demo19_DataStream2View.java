package top.doe.flinksql.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Demo19_DataStream2View {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);



        // 假设有一个流
        // [1,{"score":80,"course":"化学","ts":1000},{"score":90,"course":"物理","ts":2000}]
        // [2,{"score":80,"course":"化学","ts":3000},{"score":90,"course":"物理","ts":4000},{"score":92,"course":"语文","ts":5000}]
        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);
        SingleOutputStreamOperator<Info> infoStream = stream.flatMap(new RichFlatMapFunction<String, Info>() {
            Pattern pattern;

            @Override
            public void open(Configuration parameters) throws Exception {
                String regex = "(\\d+)|(\\{[^}]+\\})";
                // Compile the pattern
                pattern = Pattern.compile(regex);
            }

            @Override
            public void flatMap(String s, Collector<Info> collector) throws Exception {

                // 用正则，匹配输入数据
                Matcher matcher = pattern.matcher(s);

                Integer uid = null;
                // 抽取匹配结果
                while (matcher.find()) {

                    if (matcher.group(1) != null) {
                        uid = Integer.parseInt(matcher.group(1));
                    } else if (matcher.group(2) != null) {
                        String json = matcher.group(2);
                        JSONObject jsonObj = JSON.parseObject(json);
                        String course = jsonObj.getString("course");
                        double score = jsonObj.getDoubleValue("score");
                        long ts = jsonObj.getLongValue("ts");

                        // 输出一个结果
                        if (uid != null) {
                            collector.collect(new Info(uid, course, score,ts));
                        }

                    }

                }
            }
        });


        // 流转表
        tenv.createTemporaryView("tmp1",infoStream);  // 不显式指定schema，则会从流的数据类型（javaBean）中自动推断出表的schema
        tenv.executeSql("desc tmp1").print();



        tenv.createTemporaryView(
                "tmp2",
                infoStream,
                Schema.newBuilder()    // 显式指定表结构
                        .column("stu_id", DataTypes.INT())    // 物理字段
                        .column("course", DataTypes.STRING())  // 物理字段
                        .column("score", DataTypes.DOUBLE())   // 物理字段
                        .column("ts", DataTypes.BIGINT())   // 物理字段
                        .columnByExpression("score2","score+100")
                        .columnByExpression("rt","to_timestamp_ltz(ts,3)")
                        //.columnByMetadata("partition","partition")
                        .watermark("rt","rt - interval '0' second")
                        .build()
        );
        tenv.executeSql("desc tmp2").print();


        tenv.executeSql("select *,CURRENT_WATERMARK(rt) from tmp2").print();



        env.execute();



    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Info{
        private int stu_id;
        private String course;
        private double score;
        private long ts;
    }


}

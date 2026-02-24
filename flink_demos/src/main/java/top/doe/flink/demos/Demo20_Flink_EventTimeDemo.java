package top.doe.flink.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;

public class Demo20_Flink_EventTimeDemo {
    public static void main(String[] args) throws Exception {


        // 创建编程入口（环境）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 加载数据源得到流
        // 时间戳,province,金额
        // 1000,a,100
        // 2000,a,150
        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9898);

        // 解析数据
        SingleOutputStreamOperator<Od> odStream = stream.map(new MapFunction<String, Od>() {
            @Override
            public Od map(String value) throws Exception {
                String[] split = value.split(",");
                return new Od(Long.parseLong(split[0]), split[1], Integer.parseInt(split[2]));
            }
        });

        // 安插一个生成watermark的算子
        SingleOutputStreamOperator<Od> wmStream =
                odStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Od>forBoundedOutOfOrderness(Duration.ofSeconds(2))    // 选择策略：处理有界乱序的策略： 事件时间-乱序时长
                                .withTimestampAssigner(new SerializableTimestampAssigner<Od>() {  // 抽取数据中的时间的逻辑
                                    @Override
                                    public long extractTimestamp(Od od, long recordTimestamp) {
                                        return od.createTime;
                                    }
                                })
                );


        // keyBy
        KeyedStream<Od, String> keyedStream = wmStream.keyBy(od -> od.province);


        // 各省的每5秒的成交金额 （滚动时间窗口）
        SingleOutputStreamOperator<String> resultStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))  // 各窗口的数据在运行时，是存在状态中的（copyOnWriteStateMap/ rocksDB）
                        // 泛型1：输入的数据类型  泛型2：输出结果类型  泛型3：key类型  泛型4：窗口类型
                        // process是一个窗口的全量计算算子
                        .process(new ProcessWindowFunction<Od, String, String, TimeWindow>() {

                            @Override
                            public void process(String key, ProcessWindowFunction<Od, String, String, TimeWindow>.Context context, Iterable<Od> elements, Collector<String> out) throws Exception {

                                int sum = 0;

                                for (Od od : elements) {
                                    sum += od.amount;
                                }

                                HashMap<String, Object> resMap = new HashMap<>();
                                resMap.put("window_start", context.window().getStart());
                                resMap.put("window_end", context.window().getEnd());
                                resMap.put("province", key);
                                resMap.put("total_amount", sum);

                                out.collect(JSON.toJSONString(resMap));

                            }
                        });


        resultStream.print();


        env.execute();

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Od implements Serializable {
        private long createTime;
        private String province;
        private int amount;
    }
}

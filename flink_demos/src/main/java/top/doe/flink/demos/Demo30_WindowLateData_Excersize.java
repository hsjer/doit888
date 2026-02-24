package top.doe.flink.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;

public class Demo30_WindowLateData_Excersize {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");


        // {"oid":1,"uid":2,"province":"江苏省","amt":100.5,"create_time":1729127043000}
        // {"oid":1,"uid":2,"province":"江苏省","amt":100.5,"create_time":1729127043000}
        // 每5分钟的，各省的订单金额
        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);

        SingleOutputStreamOperator<Bean> beanStream = stream.map(s -> JSON.parseObject(s, Bean.class));


        // 生成 watermark
        SingleOutputStreamOperator<Bean> watermarkStream = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Bean>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Bean>() {
                            @Override
                            public long extractTimestamp(Bean bean, long recordTimestamp) {
                                return bean.create_time;
                            }
                        })

        );


        // 按省份keyBy
        KeyedStream<Bean, String> keyedStream = watermarkStream.keyBy(b -> b.province);


        // keyed滚动事件时间窗口
        OutputTag<Bean> sideTag = new OutputTag<>("late-data:", TypeInformation.of(Bean.class));


        SingleOutputStreamOperator<Result> resultStream = keyedStream.window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .allowedLateness(Time.minutes(1)) // 距离窗口触发后1分钟以内的迟到数据可以重新触发
                .sideOutputLateData(sideTag)  // 真正迟到的数据，放入一个侧输出流
                .process(new ProcessWindowFunction<Bean, Result, String, TimeWindow>() {
                    @Override
                    public void process(String province, ProcessWindowFunction<Bean, Result, String, TimeWindow>.Context context, Iterable<Bean> elements, Collector<Result> out) throws Exception {

                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        String window_start = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        String window_end = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");

                        double amount = 0;
                        for (Bean element : elements) {
                            amount += element.amt;
                        }

                        out.collect(new Result(window_start, window_end, province, amount));
                    }
                });


        // 构造一个jdbc sink 对象
        // JdbcSink构造的SinkFunction类型的sink，是一个 at least once 投递语义的 sink
        SinkFunction<Result> sink = JdbcSink.<Result>sink(
                // upsert : update & insert
                "insert into province_order_amount (window_start,window_end,province,amount) values (?, ?, ? ,?) on duplicate key update amount = ? ",

                (pst, rs) -> {
                    pst.setString(1,rs.window_start);
                    pst.setString(2,rs.window_end);
                    pst.setString(3,rs.province);
                    pst.setDouble(4,rs.amount);
                    pst.setDouble(5,rs.amount);
                },

                // JDBC 执行参数
                JdbcExecutionOptions.builder()
                        .withBatchSize(10)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),

                // JDBC 连接
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://doitedu01:3306/doit50")
                        //.withDriverName("com.mysql.driver.Con")
                        .withUsername("root")
                        .withPassword("ABC123.abc123")
                        .build()
        );


        // 把窗口计算结果，利用jdbcSink输出
        resultStream.addSink(sink);


        // 处理迟到数据
        SideOutputDataStream<Bean> lateDate = resultStream.getSideOutput(sideTag);
        // 针对这些迟到数据，去更新我们的业务结果
        lateDate.addSink(new LateSinkFunction());



        env.execute();

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    // {"oid":1,"uid":2,"province":"江苏省","amt":100.5,"create_time":1729127043000}
    public static class Bean implements Serializable {
        private int oid;
        private int uid;
        private String province;
        private double amt;
        private long create_time;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Result implements Serializable {
        private String window_start;
        private String window_end;
        private String province;
        private double amount;
    }



    public static class LateSinkFunction extends RichSinkFunction<Bean> {
        Connection conn;
        PreparedStatement queryPst;
        PreparedStatement updatePst;

        public LateSinkFunction() throws SQLException {
        }


        @Override
        public void open(Configuration parameters) throws Exception {

            conn = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/doit50", "root", "ABC123.abc123");
            queryPst = conn.prepareStatement("select amount  from province_order_amount where window_start=? and window_end =? and province=?");
            updatePst = conn.prepareStatement("update province_order_amount set amount = ? where window_start=? and window_end =? and province=?");

        }

        @Override
        public void invoke(Bean bean, Context context) throws Exception {

            long createTime = bean.create_time;
            String window_start = DateFormatUtils.format(createTime / (5 * 60 * 1000) * (5 * 60 * 1000), "yyyy-MM-dd HH:mm:ss.SSS");
            String window_end = DateFormatUtils.format((createTime+5 * 60 * 1000) / (5 * 60 * 1000) * (5 * 60 * 1000), "yyyy-MM-dd HH:mm:ss.SSS");

            String province = bean.province;
            double amt = bean.amt;


            // 先查
            queryPst.setString(1,window_start);
            queryPst.setString(2,window_end);
            queryPst.setString(3,province);
            ResultSet resultSet = queryPst.executeQuery();
            resultSet.next();
            double dbAmount = resultSet.getDouble("amount");

            // 插入更新结果
            updatePst.setDouble(1,amt+dbAmount);
            updatePst.setString(2,window_start);
            updatePst.setString(3,window_end);
            updatePst.setString(4,province);
            updatePst.executeUpdate();
        }


        @Override
        public void finish() throws Exception {
            updatePst.close();
            queryPst.close();
            conn.close();

        }
    }

}

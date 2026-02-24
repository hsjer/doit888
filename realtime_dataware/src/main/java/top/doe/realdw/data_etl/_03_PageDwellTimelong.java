package top.doe.realdw.data_etl;

import com.alibaba.fastjson2.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import top.doe.realdw.utils.EnvUtil;

import java.io.Serializable;
import java.util.Map;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/26
 * @Desc: 学大数据，上多易教育
 * 访问时长分析主题： 页面停留时长分析
 **/
public class _03_PageDwellTimelong {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = EnvUtil.getEnv();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 读dwd层kafka中的行为明细宽表数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu01:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics("dwd-events")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setGroupId("g001")
                .setClientIdPrefix("cli")
                .build();

        DataStreamSource<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "明细数据");

        SingleOutputStreamOperator<UserEvent> events = streamSource.map(new MapFunction<String, UserEvent>() {
            @Override
            public UserEvent map(String json) throws Exception {
                return JSON.parseObject(json, UserEvent.class);
            }
        });


        SingleOutputStreamOperator<UserEvent> processed = events.keyBy(e -> e.getSession_id())
                .process(new KeyedProcessFunction<String, UserEvent, UserEvent>() {
                    ValueState<UserEvent> st;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        ValueStateDescriptor<UserEvent> desc = new ValueStateDescriptor<>("st", UserEvent.class);
                        desc.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.minutes(120))
                                        .updateTtlOnReadAndWrite()
                                        .build());

                        st = getRuntimeContext().getState(desc);
                    }

                    @Override
                    public void processElement(UserEvent currentEvent, KeyedProcessFunction<String, UserEvent, UserEvent>.Context ctx, Collector<UserEvent> out) throws Exception {

                        /*
                            u01,s1,t01,launch,null
                            u01,s1,t02,pgload,index
                            u01,s1,t03,event1,index
                            u01,s1,t04,event2,index
                            u01,s1,t05,pgload,page02
                         */
                        if (currentEvent.getEvent_id().equals("page_load")) {
                            // page_load，代表新页面开始了，所以先填充好  ：页面起始时间，和 事件所在页面
                            currentEvent.setPage(currentEvent.getProperties().get("url"));
                            currentEvent.setPage_start_time(currentEvent.action_time);

                            // 修改状态中数据的 action_time，其他字段不变，输出==> 为了给上一个页面提供一个结束时间
                            UserEvent stateEvent = st.value();
                            if (stateEvent == null) {
                                st.update(currentEvent);
                            } else {
                                stateEvent.setAction_time(currentEvent.action_time);
                                out.collect(stateEvent);  // 把状态中修改过时间的数据输出

                                st.update(currentEvent);  // 把新页面信息，覆盖掉状态中的数据
                            }

                            out.collect(currentEvent);  // 把新页面的信息也输出
                        } else if (currentEvent.getEvent_id().equals("wake_up")) {
                            // 换掉状态中的  “所在页面起始时间”，然后输出
                            UserEvent stateEvent = st.value();
                            stateEvent.setPage_start_time(currentEvent.action_time);
                            stateEvent.setAction_time(currentEvent.action_time);

                            out.collect(stateEvent);
                        } else {
                            // 普通的事件,把状态中的数据的 action_time 更新掉，然后输出
                            UserEvent stateEvent = st.value();
                            if (stateEvent != null) {
                                stateEvent.setAction_time(currentEvent.action_time);
                                out.collect(stateEvent);
                            }

                        }

                        if(currentEvent.getEvent_id().equals("app_close")){
                            st.clear();
                        }


                    }
                });

        processed.print();



        // 把上面的结果 ，流转表
        Table table = tenv.fromDataStream(processed, Schema.newBuilder()
                        .column("user_id", DataTypes.INT())
                        .column("session_id", DataTypes.STRING())
                        .column("event_id", DataTypes.STRING())
                        .column("properties", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                        .column("action_time", DataTypes.BIGINT())
                        .column("release_channel", DataTypes.STRING())
                        .column("province", DataTypes.STRING())
                        .column("city", DataTypes.STRING())
                        .column("region", DataTypes.STRING())
                        .column("page", DataTypes.STRING())
                        .column("page_start_time", DataTypes.BIGINT())
                        .columnByExpression("rt", "to_timestamp_ltz(action_time,3)")
                        .watermark("rt","rt")
                .build());
        tenv.createTemporaryView("tmp", table);


        // 建表，映射doris
        tenv.executeSql(
                        " create table doris_sink(                                 "+
                        "     dt date,                                             "+
                        "     user_id bigint,                                      "+
                        "     session_id string,                                   "+
                        "     province string,                                     "+
                        "     city string,                                         "+
                        "     region string,                                       "+
                        "     page string,                                         "+
                        "     page_start_time bigint,                              "+
                        "     page_end_time bigint                                 "+
                        " ) with (                                                 "+
                        "     'connector' = 'doris',                               "+
                        "     'fenodes' = 'doitedu01:8030',                        "+
                        "     'table.identifier' = 'dws.actionlog_access_dwell',   "+
                        "     'username' = 'root',                                 "+
                        "     'password' = '123456',                               "+
                        "     'sink.label-prefix' = 'doris_label-004'              "+
                        " )                                                        "
        );


        tenv.executeSql(
                "insert into doris_sink " +
                "select TO_DATE(date_format(to_timestamp_ltz(page_start_time,3),'yyyy-MM-dd')) as dt," +
                " user_id,session_id,province,city,region,page,page_start_time, \n" +
                "max(action_time) as page_end_time                              \n" +
                "from table(                                                    \n" +
                "    tumble(table tmp,descriptor(rt),interval '1' minute)       \n" +
                ")                                                              \n" +
                "group by window_start,window_end,province,city,region,user_id,session_id,page,page_start_time").print();

        env.execute();

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserEvent implements Serializable {

        private int user_id;
        private String session_id;
        private String event_id;
        private Map<String, String> properties;
        private long action_time;
        private String release_channel;
        private String province;
        private String city;
        private String region;
        //private String job = "";

        private String page;
        private long page_start_time;

    }


}

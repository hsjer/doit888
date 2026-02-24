package top.doe;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import top.doe.bean.RuleMeta;
import top.doe.bean.UserEvent;
import top.doe.calculator_model.RuleCalculator;
import top.doe.functions.RuleCalculateCoreFunction;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/17
 * @Desc: 学大数据，上多易教育
 **/

@Slf4j
public class RuleEngineVersion4_1 {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        env.setParallelism(2);

        // 读用户行为数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("doitedu01:9092,doitedu02:9092,doitedu03:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setClientIdPrefix("rule_engine_cli_")
                .setGroupId("rule_engine_group_")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setTopics("user_event_log")
                .build();

        DataStreamSource<String> eventSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "event_source");

        // 解析json数据
        SingleOutputStreamOperator<UserEvent> eventStream = eventSource.map(json -> JSON.parseObject(json, UserEvent.class));

        // 省略关联维度的阶段

        // keyBy
        KeyedStream<UserEvent, String> keyedEvent = eventStream.keyBy(UserEvent::getAccount);


        // 监听规则元数据表( 增删改 )
        // 创建mysql cdc source 对象
        MySqlSource<String> cdcSource = MySqlSource.<String>builder()
                .username("root")
                .password("ABC123.abc123")
                .hostname("doitedu01")
                .port(3306)
                .databaseList("dw_50")
                .tableList("dw_50.rule_metadata")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> cdcStream = env.fromSource(cdcSource, WatermarkStrategy.noWatermarks(), "mysql-cdc");

        // 解析cdc数据
        SingleOutputStreamOperator<RuleMeta> ruleMetaStream = cdcStream.map(json -> {
            JSONObject jsonObject = JSON.parseObject(json);
            String op = jsonObject.getString("op");

            // 获取cdc数据中的after：包含表数据
            JSONObject data;
            if (op.equals("d")) {
                data = jsonObject.getJSONObject("before");
            } else {
                data = jsonObject.getJSONObject("after");
            }

            // 把数据，封装到RuleMeta对象中
            RuleMeta ruleMeta = JSON.parseObject(data.toJSONString(), RuleMeta.class);
            // 从中取出bitmap字节，反序列化成 bitmap对象
            byte[] ruleCrowdBitmapBytes = ruleMeta.getRule_crowd_bitmap_bytes();
            Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf();
            bitmap.deserialize(ByteBuffer.wrap(ruleCrowdBitmapBytes));

            // 再把bitmap对象，和op，放入 ruleMeta中
            ruleMeta.setRule_crowd_bitmap(bitmap);
            ruleMeta.setOp(op);


            return ruleMeta;
        });


        // 广播元数据流
        MapStateDescriptor<Integer, RuleCalculator> bcStateDesc = new MapStateDescriptor<>("calculator_pool", Integer.class, RuleCalculator.class);
        BroadcastStream<RuleMeta> ruleMetaBroadcastStream = ruleMetaStream.broadcast(bcStateDesc);


        // 连接事件流  和  元数据广播流
        SingleOutputStreamOperator<String> messages = keyedEvent.connect(ruleMetaBroadcastStream)
                .process(new RuleCalculateCoreFunction());


        messages.print();


        env.execute();


    }
}

package top.doe;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import top.doe.bean.RuleMeta;
import top.doe.bean.UserEvent;
import top.doe.calculator_model.RuleCalculator;
import top.doe.functions.BenchSink;
import top.doe.functions.MetaParseMapFunction;
import top.doe.functions.RuleCalculateCoreFunction;
import top.doe.functions.Unorder2OrderFunction;
import top.doe.utils.SourceStreamUtil;
import top.doe.utils.StateDesc;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/17
 * @Desc: 学大数据，上多易教育
 **/

@Slf4j
public class RuleEngineVersion5_1 {

    public static void main(String[] args) throws Exception {

        // 获取编程入口环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        //env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        //env.setParallelism(1);


        /* 一、读用户行为数据 */
        //DataStreamSource<String> eventSource = SourceStreamUtil.getStreamFromKafka(env);
        DataStreamSource<String> eventSource = SourceStreamUtil.getStreamFromDataGen(env);


        /* 一、监听规则元数据表( 增删改 ) */
        DataStreamSource<String> cdcStream = SourceStreamUtil.getMetaFromCdc(env);


        /* 三、解析元数据 */
        SingleOutputStreamOperator<RuleMeta> ruleMetaStream =
                cdcStream.name("元数据cdc").slotSharingGroup("cdc")
                        .map(new MetaParseMapFunction()).name("元数据解析").slotSharingGroup("cdc");


        /* 四、广播元数据 */
        BroadcastStream<RuleMeta> ruleMetaBroadcastStream = ruleMetaStream.broadcast(StateDesc.BC_STATE_DESC);


        /* 五、解析用户行为 */
        SingleOutputStreamOperator<UserEvent> eventStream =
                eventSource.name("行为日志source").slotSharingGroup("events")
                        .map(json -> JSON.parseObject(json, UserEvent.class)).name("行为日志解析").slotSharingGroup("events");

        /* 六、解析用户行为 */


        /* 七、用户行为数据，按照用户id keyBy */
        KeyedStream<UserEvent, Long> keyedEvent1 = eventStream.keyBy(UserEvent::getUser_id);


        /* 八、解决乱序问题 */
        SingleOutputStreamOperator<UserEvent> orderedStream = keyedEvent1.process(new Unorder2OrderFunction()).name("乱序处理").slotSharingGroup("unordered");

        /* 九、把乱序处理后的数据，重新keyBy */
        KeyedStream<UserEvent, Long> keyedEvent2 = orderedStream.keyBy(e -> e.getUser_id());

        //
        /* 十、连接事件流  和  元数据广播流 */
        SingleOutputStreamOperator<String> messages = keyedEvent2.connect(ruleMetaBroadcastStream)
                .process(new RuleCalculateCoreFunction()).slotSharingGroup("core");


        // benchMark  ： 性能测试
        messages.addSink(new BenchSink()).disableChaining().slotSharingGroup("events");


        env.execute();
    }
}

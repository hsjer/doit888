package top.doe.flink.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/10/11
 * @Desc: 学大数据，上多易教育
 * 实时捕获业务系统上的营收表，实时统计各性别的平均营收
 **/

@Slf4j
public class Demo11_Flink_ValueStateDemo {
    //private static Logger logger = LoggerFactory.getLogger(Demo10_MySqlCdcSource_Excersize.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/devworks/doit50_hadoop");
        env.setParallelism(2);

        // 创建source
        MySqlSource<String> source = MySqlSource.<String>builder()
                .username("root")
                .password("ABC123.abc123")
                .hostname("doitedu01")
                .port(3306)
                .databaseList("doit50")
                .tableList("doit50.t_person")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "xx");

        // json 解析
        SingleOutputStreamOperator<CdcBean> beanStream = stream.map(new MapFunction<String, CdcBean>() {
            @Override
            public CdcBean map(String cdcJson) throws Exception {

                return JSON.parseObject(cdcJson, CdcBean.class);
            }
        });




        // keyBy
        KeyedStream<CdcBean, String> keyedStream = beanStream.keyBy(new KeySelector<CdcBean, String>() {
            @Override
            public String getKey(CdcBean bean) throws Exception {
                return bean.after != null ? bean.after.gender : bean.before.gender;
            }
        });


        // process算子是一个灵活的算子，没有固定某种处理模型；而是完全交给用户来编写数据处理逻辑
        SingleOutputStreamOperator<String> resultStream = keyedStream.process(new KeyedProcessFunction<String, CdcBean, String>() {

            JSONObject res = new JSONObject();

            // 状态数据存储在内存集合、变量中是不可靠的，无法实现容错性（崩溃后的恢复）
            //HashMap<String, Pair> mapState = new HashMap<>();

            MapState<String, Pair> mapState;

            ValueState<Pair> valueState;


            // RichFunction中定义的生命周期方法
            // task在初始化的时候，会调用一次这个open
            @Override
            public void open(Configuration parameters) throws Exception {

                RuntimeContext runtimeContext = getRuntimeContext();

                // 定义mapState状态的描述信息
                MapStateDescriptor<String, Pair> mapStateDesc = new MapStateDescriptor<>("mapState", String.class, Pair.class);

                // 通过runtimeContext获取一个状态容器
                mapState = runtimeContext.getMapState(mapStateDesc);


                valueState = runtimeContext.getState(new ValueStateDescriptor<Pair>("valueState", Pair.class));


            }



            @Override
            public void processElement(CdcBean cdcBean, KeyedProcessFunction<String, CdcBean, String>.Context ctx, Collector<String> out) throws Exception {


                // 每次操作的valueState，看似是同一个，其实底层会根据当前上下文中key自动切换数据
                Pair statePair = valueState.value();

                if(statePair == null) {
                    statePair = new Pair(0,0.0);
                    valueState.update(statePair);
                }


                // 统计各性别的 平均营收
                SalaryInfo before = cdcBean.before;
                SalaryInfo after = cdcBean.getAfter();
                String op = cdcBean.op;

                // 方法的ctx上下中，持有了当前数据所属的 key
                String gender = ctx.getCurrentKey();

                if (op.equals("r") || op.equals("c")) {
                    // 把状态中的记录的值，更新： cnt++,  sum+=
                    statePair.cnt ++;
                    statePair.sum += after.salary;
                } else if (op.equals("u")) {
                    // 计算更新前和更新后的金额的差值
                    double diff = after.salary - before.salary;
                    statePair.sum += diff;

                } else if (op.equals("d")) {

                    statePair.sum -= before.salary;
                    statePair.cnt --;

                } else {
                    log.error("有点小问题，不应该有这样的op:{}", op);
                }

                if (statePair.cnt > 0) {
                    res.put("gender", gender);
                    res.put("avg_salary", statePair.sum / statePair.cnt);

                    out.collect(res.toJSONString());
                }
            }
        });


        resultStream.print();


        env.execute();
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Pair implements Serializable {
        private int cnt;
        private double sum;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SalaryInfo implements Serializable {
        private int id;
        private String name;
        private String gender;
        private double salary;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CdcBean implements Serializable {
        private SalaryInfo before;
        private SalaryInfo after;
        private String op;

    }


}

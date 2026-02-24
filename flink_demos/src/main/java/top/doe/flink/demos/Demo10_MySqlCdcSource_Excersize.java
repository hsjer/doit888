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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/10/11
 * @Desc: 学大数据，上多易教育
 * 实时捕获业务系统上的营收表，实时统计各性别的平均营收
 **/

@Slf4j
public class Demo10_MySqlCdcSource_Excersize {
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

//        beanStream.process(new ProcessFunction<CdcBean, String>() {
//            @Override
//            public void processElement(CdcBean value, ProcessFunction<CdcBean, String>.Context ctx, Collector<String> out) throws Exception {
//            }
//        });



        // keyBy
        KeyedStream<CdcBean, String> keyedStream = beanStream.keyBy(new KeySelector<CdcBean, String>() {
            @Override
            public String getKey(CdcBean bean) throws Exception {
                return bean.after != null ? bean.after.gender : bean.before.gender;
            }
        });


        // process算子是一个灵活的算子，没有固定某种处理模型；而是完全交给用户来编写数据处理逻辑
        SingleOutputStreamOperator<String> resultStream = keyedStream.process(new KeyedProcessFunction<String, CdcBean, String>() {

            HashMap<String, Pair> mapState = new HashMap<>();
            JSONObject res = new JSONObject();

            @Override
            public void processElement(CdcBean cdcBean, KeyedProcessFunction<String, CdcBean, String>.Context ctx, Collector<String> out) throws Exception {

                // 统计各性别的 平均营收
                SalaryInfo before = cdcBean.before;
                SalaryInfo after = cdcBean.getAfter();
                String op = cdcBean.op;

                // 方法的ctx上下中，持有了当前数据所属的 key
                String gender = ctx.getCurrentKey();
                //String gender = after != null ? after.gender : before.gender;

                if (op.equals("r") || op.equals("c")) {

                    Pair stateValue = mapState.getOrDefault(gender, new Pair(0, 0.0));

                    stateValue.cnt++;
                    stateValue.sum += after.salary;

                    mapState.put(gender, stateValue);

                } else if (op.equals("u")) {
                    // 计算更新前和更新后的金额的差值
                    double diff = after.salary - before.salary;

                    Pair stateValue = mapState.get(gender);

                    if (stateValue == null) throw new RuntimeException("有问题有问题，大大的有问题");

                    stateValue.sum += diff;

                } else if (op.equals("d")) {

                    Pair stateValue = mapState.get(gender);
                    if (stateValue == null) throw new RuntimeException("有问题有问题，大大的有问题");

                    stateValue.sum -= before.salary;
                    stateValue.cnt--;

                } else {
                    log.error("有点小问题，不应该有这样的op:{}", op);
                }

                Pair pair = mapState.get(gender);
                if (pair != null && pair.cnt != 0) {
                    res.put("gender", gender);
                    res.put("avg_salary", pair.sum / pair.cnt);

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

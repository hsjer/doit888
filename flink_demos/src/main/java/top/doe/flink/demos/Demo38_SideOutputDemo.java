package top.doe.flink.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/10/20
 * @Desc: 学大数据，上多易教育
 *   侧流输出 api 示例
 **/
public class Demo38_SideOutputDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);


        OutputTag<String> zsjTag = new OutputTag<String>("zsj"){};


        SingleOutputStreamOperator<JSONObject> resultStream = stream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String json, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(json);
                    out.collect(jsonObject);  // 主流输出
                } catch (Exception e) {
                    // 侧流输出脏数据
                    context.output(zsjTag, json);
                }
            }
        });

        resultStream.print("正常数据:");


        SideOutputDataStream<String> zsjStream = resultStream.getSideOutput(zsjTag);

        zsjStream.print("脏数据:");


        env.execute();
    }


}

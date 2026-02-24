package top.doe.flink.demos;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo16_Flink_KeyedStateTTL {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        // 设置状态后端为  EmbeddedRocksDBStateBackend ，true代表开启增量快照功能
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));  // 默认用的是HashMapStateBackend


        DataStreamSource<String> stream = env.socketTextStream("doitedu01", 9999);


        stream.keyBy(s->s).map(new RichMapFunction<String, String>() {

            ListState<String> listState;


            @Override
            public void open(Configuration parameters) throws Exception {

                RuntimeContext runtimeContext = getRuntimeContext();


                // 创建一个TTL配置参数对象
                StateTtlConfig ttlConfig = new StateTtlConfig
                        .Builder(Time.seconds(5))
                        .neverReturnExpired()  // 永远不要让用户看见已过期的数据
                        //.updateTtlOnReadAndWrite()  // 只要listState中某条数据被读过或写过，就更新它的TTL，从0开始计时
                        //.updateTtlOnCreateAndWrite()  // 只要listState中某条数据被创建或写过，就更新它的TTL，从0开始计时
                        .cleanupFullSnapshot()  // 在做全量快照的时候，不要把过期数据放入快照中
                        //.disableCleanupInBackground() // 禁用task运行过程中的后台自动清除过期数据线程（清除不会彻底，但一般不要禁用）
                        //.cleanupInRocksdbCompactFilter(1000) // 如果状态数据使用rocksdb来存储，则当rocksdb做compaction时清理过期状态数据
                        .build();

                // 创建state定义对象
                ListStateDescriptor<String> desc = new ListStateDescriptor<>("seq", String.class);
                // 为state定义开启TTL，并传入TT了参数
                desc.enableTimeToLive(ttlConfig);

                // 利用state描述，获取listState
                listState = runtimeContext.getListState(desc);

            }

            @Override
            public String map(String value) throws Exception {


                listState.add(value);

                System.out.print("此刻，listState: ");

                for (String s : listState.get()) {
                    System.out.print(s);
                }
                System.out.println("-------------------------");

                return value;
            }

        }).print();

        env.execute();




    }
}

package top.doe.realdw.utils;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class EnvUtil {
    public static StreamTableEnvironment getTableEnv() {

        StreamExecutionEnvironment env = getEnv();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        return tenv;
    }

    public static StreamExecutionEnvironment getEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        // 参数： execution.checkpointing.timeout = 默认 10min
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // execution.checkpointing.tolerable-failed-checkpoints = 0 未生效
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        env.setParallelism(1);


        return env;
    }


}

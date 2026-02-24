package top.doe.flinksql.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo1_Rumen {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // {"uid":1,"event_id":"aaa","properties":{"url":"/a","ref":"/x"},"action_time":1704719574000}
        tenv.executeSql(
                "CREATE TABLE t_rumen (          \n" +               // 物理字段
                "   uid BIGINT                    \n" +                 // 物理字段
                "  ,event_id STRING              \n" +               // 物理字段
                "  ,properties MAP<STRING,STRING>\n" +               // 物理字段
                "  ,action_time BIGINT           \n" +               // 物理字段

                "  ,uid_2 as uid+10              \n" +               // 表达式字段
                "  ,con as 10                    \n" +               // 表达式字段
                "  ,ts as to_timestamp_ltz(action_time,3)  \n" +      // 表达式字段
                "  ,action_ts as to_timestamp_ltz(action_time,3)  \n" +      // 表达式字段

                "  ,tpc_name string metadata from 'topic'  \n" +      // 连接器元数据字段
                "  ,partition_id int metadata from 'partition'  \n" +  // 连接器元数据字段
                "  ,off_set bigint metadata from 'offset'      \n" +   // 连接器元数据字段


                "  ,pt as proctime()      \n" +   // 处理时间语义的时间字段
                "  ,watermark for ts as ts - interval '0' second      \n" +   // 处理事件语义的时间字段
                ")                               \n" +
                "WITH (                          \n" +
                "  'connector' = 'kafka',        \n" +
                "  'topic' = 'od',               \n" +
                "  'properties.bootstrap.servers' = 'doitedu01:9092,doitedu02:9092,doitedu03:9092',\n" +
                "  'properties.group.id' = 'g001',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'value.format' = 'json',       \n" +
                "  'value.fields-include' = 'EXCEPT_KEY'\n" +
                ")");


        // 定义了时间语义的时间字段，字段类型有特别标记  *ROWTIME*  /  *PROCTIME*
        tenv.executeSql("desc t_rumen").print();
        /*
         *+--------------+-----------------------------+-------+-----+---------------------------------------+----------------------------+
         *|         name |                        type |  null | key |                                extras |                  watermark |
         *+--------------+-----------------------------+-------+-----+---------------------------------------+----------------------------+
         *|          uid |                      BIGINT |  TRUE |     |                                       |                            |
         *|     event_id |                      STRING |  TRUE |     |                                       |                            |
         *|   properties |         MAP<STRING, STRING> |  TRUE |     |                                       |                            |
         *|  action_time |                      BIGINT |  TRUE |     |                                       |                            |
         *|        uid_2 |                      BIGINT |  TRUE |     |                         AS `uid` + 10 |                            |
         *|          con |                         INT | FALSE |     |                                 AS 10 |                            |
         *|           ts |  TIMESTAMP_LTZ(3) *ROWTIME* |  TRUE |     | AS TO_TIMESTAMP_LTZ(`action_time`, 3) | `ts` - INTERVAL '0' SECOND |
         *|    action_ts |            TIMESTAMP_LTZ(3) |  TRUE |     | AS TO_TIMESTAMP_LTZ(`action_time`, 3) |                            |
         *|     tpc_name |                      STRING |  TRUE |     |                 METADATA FROM 'topic' |                            |
         *| partition_id |                         INT |  TRUE |     |             METADATA FROM 'partition' |                            |
         *|      off_set |                      BIGINT |  TRUE |     |                METADATA FROM 'offset' |                            |
         *|           pt | TIMESTAMP_LTZ(3) *PROCTIME* | FALSE |     |                         AS PROCTIME() |                            |
         *+--------------+-----------------------------+-------+-----+---------------------------------------+----------------------------+
         *
         */


        //tenv.executeSql("select * from t_rumen").print();
        //tenv.executeSql("select event_id,count(1) as cnt,count(distinct uid) as uv from t_rumen group by event_id").print();

    }
}

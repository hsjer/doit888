package top.doe.realdw.data_etl;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.doe.realdw.utils.EnvUtil;

public class _06_广告点击率预估_特征数据加工 {
    public static void main(String[] args) {

        StreamTableEnvironment tenv = EnvUtil.getTableEnv();

        // 建表，映射行为日志明细数据
        tenv.executeSql("create table dwd_events_kafka(      \n" +
                "     user_id bigint                         \n" +
                "    ,action_time        bigint              \n" +
                "    ,event_id           string              \n" +
                "    ,properties         map<string,string>  \n" +
                "    ,rt as to_timestamp_ltz(action_time,3) " +
                "    ,watermark for rt as rt   " +

                ") WITH (                   \n" +
                "    'connector' = 'kafka', \n" +
                "    'topic' = 'dwd-events',\n" +
                "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                "    'properties.group.id' = 'doit50_g1',  \n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json',              \n" +
                "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                ")  ");


        // 模式匹配：  一个用户的一个广告曝光后，有发生点击
        tenv.executeSql(
                        " create temporary view  show_click as                        " +
                        "with tmp as (                                            \n   "+
                        "     select                                               \n   "+
                        "         user_id,                                         \n   "+
                        "         event_id,                                        \n   "+
                        "         action_time,                                     \n   "+
                        "         properties['ad_id'] as ad_id,                    \n   "+
                        "         properties['tracking_id'] as tracking_id,        \n   "+
                        "         rt                                                \n   "+
                        "     from dwd_events_kafka                                \n   "+
                        "     where event_id in ('ad_show','ad_click')             \n   "+
                        " )                                                        \n   "+
                        "                                                          \n   "+
                        " SELECT                                                   \n   "+
                        "   uid,                                                   \n   "+
                        "   adid,                                                  \n   "+
                        "   show_time,                                             \n   "+
                        "   click_time,                                            \n   "+
                        // 生成一个处理时间语义的时间字段
                        "   proctime() as pt,                                      \n   "+
                        " FROM tmp                                                 \n   "+
                        " MATCH_RECOGNIZE(                                         \n   "+
                        "     PARTITION BY user_id,tracking_id                     \n   "+
                        "     ORDER BY rt                                          \n   "+
                        "     MEASURES                                             \n   "+
                        "        A.user_id as uid,   -- 用户id                      \n  "+
                        "        A.ad_id as adid,    -- 广告id                      \n  "+
                        "        A.action_time as show_time,  -- 曝光时间            \n  "+
                        "        B.action_time as click_time  -- 点击时间            \n  "+
                        "     ONE ROW PER MATCH                                    \n   "+
                        "     AFTER MATCH SKIP PAST LAST ROW                       \n   "+
                        "     PATTERN (A B)  WITHIN INTERVAL '2' MINUTE            \n   "+
                        "     DEFINE                                               \n   "+
                        "         A as  A.event_id = 'ad_show',                    \n   "+
                        "         B as  B.event_id = 'ad_click'                    \n   "+
                        " )                                                        \n   "
        );


        // 建表，映射 hbase中的请求特征流数据表
        // rowkey: tracking_id
        // family: f
        // 列：   features : 大json {"ad_tracking_id":"track1","requestTime":1657329650000,"platform":"app","pageId":"page001","locationType":"侧边栏","mediaType":"图文","ad_ids":[1,3,5],"ad_features":[[5,8,6,10,32,10,14,16],[2,4,3,12,21,14,18,10],[7,6,2,30,12,27,24,36]],"user_features":[28,5000,30,40,18,6,10,32,66,70],"device_features":[1024,768,2000,26,54,29],"poster_features":[10,6,78,400,600,256,2,8]}
        tenv.executeSql(
                " create table ad_request_features (                   "+
                        "      tracking_id string                      "+
                        "      ,f  ROW<features string>                "+
                        "    ,primary key(username) not enforced       "+
                        " ) WITH (                                     "+
                        "  'connector' = 'hbase-2.2',                  "+
                        "  'table-name' = 'ad_request_log',             "+
                        "  'zookeeper.quorum' = 'doitedu01:2181'       "+
                        " )                                            "
        );


        // 然后把此前的 “流内事件关联”结果，  去 lookup join  “请求特征日志”
        tenv.executeSql(
                        " select                                                                        "+
                        "    a.uid,                                                                     "+
                        "    a.adid,                                                                    "+
                        "    a.show_time,                                                               "+
                        "    a.click_time,                                                              "+
                        "    b.f.features as features                                                   "+
                        " from show_click a  join ad_request_features for system_time as of a.pt as b   "+
                        " on a.tracking_id = b.tracking_id                                              "
        );
    }
}

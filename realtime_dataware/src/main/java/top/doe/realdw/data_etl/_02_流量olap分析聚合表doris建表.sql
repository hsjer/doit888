/*
    之所以创建这样一个聚合表模型，
    是因为：在flinkSql中，如果直接按目标维度进行groupBy聚合，它每输入一条数据，就输出 -U/+U
          这样一来，写doris的数据量很大，频繁的update效率低下
    所以，我们在flinkSql中，分组聚合时，除了目标维度外，还要开滚动窗口，但是滚动窗口的最终写入doris的结果，去掉窗口字段
         这样一来，flink输出的数据就变小了，而且不存在-U/+U了
         但是，同维度值的结果数据，可能因为窗口的存在而变成多条，那么需要在doris中进行聚合
 */

DROP TABLE IF EXISTS  dws.actionlog_traffic_agg_01;
create table dws.actionlog_traffic_agg_01(
     dt               datev2
    ,time_60m         datetime
    ,time_30m         datetime
    ,time_10m         datetime
    ,time_05m         datetime
    ,user_id          bigint
    ,register_dt      datev2
    ,session_id       varchar(12)
    ,release_channel  varchar(20)
    ,device_type      varchar(20)
    ,gps_province     varchar(20)
    ,gps_city         varchar(20)
    ,gps_region       varchar(20)
    ,page_type        varchar(20)
    ,page_url         varchar(20)
    ,pv_amt           bigint  SUM
)
AGGREGATE
KEY(
    dt
    ,time_60m
    ,time_30m
    ,time_10m
    ,time_05m
    ,user_id
    ,register_dt
    ,session_id
    ,release_channel
    ,device_type
    ,gps_province
    ,gps_city
    ,gps_region
    ,page_type
    ,page_url
)
PARTITION BY RANGE(dt)
(
)
DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES
(
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "2"
);
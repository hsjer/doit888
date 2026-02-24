DROP TABLE IF EXISTS  dws.actionlog_access_dwell;
create table dws.actionlog_access_dwell(
     dt datev2,
     user_id bigint,
     session_id varchar(40),
     province varchar(40),
     city varchar(40),
     region varchar(40),
     page varchar(40),
     page_start_time bigint,
     page_end_time bigint  MAX
)
AGGREGATE
KEY(
    dt
    ,user_id
    ,session_id
    ,province
    ,city
    ,region
    ,page
    ,page_start_time
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
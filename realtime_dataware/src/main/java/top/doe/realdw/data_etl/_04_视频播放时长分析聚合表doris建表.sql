DROP TABLE IF EXISTS  dws.actionlog_video_play;
create table dws.actionlog_video_play(
      dt datev2,
      province varchar(20),
      city varchar(20),
      region varchar(20),
      device_type varchar(20),
      release_channel varchar(20),
      user_id  bigint,      -- 哪个人
      play_id  varchar(20), -- 哪次播放
      video_id bigint,      -- 哪个视频
      segment_id  int,      -- 第几段
      start_time bigint MIN,  -- 段落起始
      end_time bigint  MAX

)
AGGREGATE
KEY(
    dt
    ,province
    ,city
    ,region
    ,device_type
    ,release_channel
    ,user_id
    ,play_id
    ,video_id
    ,segment_id
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
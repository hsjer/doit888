-- ods层用于接收datax增量同步数据的表
create table ods.ums_member_incr (
     id bigint
    ,member_level_id bigint
    ,username string
    ,password string
    ,nickname string
    ,phone string
    ,status int
    ,create_time timestamp
    ,icon string
    ,gender int
    ,birthday date
    ,city string
    ,job string
    ,personalized_signature string
    ,source_type int
    ,integration int
    ,growth int
    ,luckey_count int
    ,history_integration int
    ,modify_time timestamp
)
    partitioned by (dt string)
stored as orc 
tblproperties(
    'orc.compress'='snappy'
);


-- dwd层用于存储ums_member数据的拉链表
create table dwd.ums_member_scd (
     id bigint
    ,member_level_id bigint
    ,username string
    ,password string
    ,nickname string
    ,phone string
    ,status int
    ,create_time timestamp
    ,icon string
    ,gender int
    ,birthday date
    ,city string
    ,job string
    ,personalized_signature string
    ,source_type int
    ,integration int
    ,growth int
    ,luckey_count int
    ,history_integration int
    ,modify_time timestamp
    ,start_dt string
    ,end_dt   string
)
    partitioned by (dt string)
stored as orc 
tblproperties(
    'orc.compress'='snappy'
);


-- 造测试数据

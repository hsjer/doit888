create table dwd.user_events_detail     (
     event_id                            varchar(100)
    ,province                           varchar(100)
    ,city                               varchar(100)
    ,region                             varchar(100)

    ,dt                                 datev2
    ,release_channel                    varchar(100)
    ,device_type                        varchar(100)
    ,session_id                         varchar(100)
    ,lat                                double
    ,lng                                double
    ,user_name                          varchar(100)
    ,action_time                        bigint
    ,properties                         map<string,string>
    ,user_id                            bigint
    ,member_level_id                    int
    ,nickname                           varchar(100)
    ,phone                              varchar(100)
    ,status                             int
    ,create_time                        datetime
    ,icon                               varchar(100)
    ,gender                             int
    ,birthday                           date
    ,register_city                      varchar(100)
    ,job                                varchar(100)
    ,personalized_signature             varchar(100)
    ,source_type                        int
    ,integration                        int
    ,growth                             int
    ,luckey_count                       int
    ,history_integration                int
    ,modify_time                        datetime
    ,page_type                          varchar(100)
    ,page_service                       varchar(100)
    ,page_channel                       varchar(100)
    ,page_lanmu                         varchar(100)

)
    DUPLICATE
KEY(event_id,province,city,region)
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
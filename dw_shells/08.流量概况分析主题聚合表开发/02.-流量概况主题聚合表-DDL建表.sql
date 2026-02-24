create table dws.actionlog_traffic_aggregate_day
(
    session_id         string,
    guid               bigint,
    member_level       int,
    device_type        string,
    enter_province     string,
    exit_province      string,
    enter_page         string,
    enter_page_type    string,
    exit_page          string,
    exit_page_type     string,
    session_start_time bigint,
    session_end_time   bigint,
    session_amt_pv     bigint,
    session_amt_time   bigint
)
    partitioned by (dt string)
    stored as orc
    tblproperties (
        'orc.compress' = 'snappy'
        );



-- 建doris外部表来映射
CREATE TABLE `hive_actionlog_traffic_aggregate_day` (
         session_id         string,
         guid               bigint,
         member_level       int,
         device_type        string,
         enter_province     string,
         exit_province      string,
         enter_page         string,
         enter_page_type    string,
         exit_page          string,
         exit_page_type     string,
         session_start_time bigint,
         session_end_time   bigint,
         session_amt_pv     bigint,
         session_amt_time   bigint
) ENGINE=HIVE
COMMENT "HIVE"
PROPERTIES (
'hive.metastore.uris' = 'thrift://doitedu01:9083',
'database' = 'dws',
'table' = 'actionlog_traffic_aggregate_day'
);
-- 在doris中，创建一个hive的catalog
CREATE CATALOG hive PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://doitedu01:9083',
    'hadoop.username' = 'root',
    'fs.defaultFS' = 'hdfs://doitedu01:8020/'
);

-- 建好catalog后，就可以直接doris中查询hive的表了，只不过要在表名前添加catalog路径
-- 设备类型	会员等级	会话总数	会话总时长	最大会话时长	最小会话时长	pv总数	最大会话pv数	最小会话pv数	跳出会话数	跳出率
select device_type,member_level,
       count(1) as session_cnt,
       sum(session_amt_time) as session_amt_time,
       max(session_amt_time) as session_max_time,
       min(session_amt_time) as session_min_time,
       sum(session_amt_pv)   as session_amt_pv,
       max(session_amt_pv)   as session_max_pv,
       min(session_amt_pv)   as session_min_pv,
       sum(if(session_amt_pv<2,1,0)) as session_jump_cnt,
       sum(if(session_amt_pv<2,1,0))/ count(1)  as session_jump_ratio
from hive.dws.actionlog_traffic_aggregate_day
where dt='20241101'
group by device_type,member_level
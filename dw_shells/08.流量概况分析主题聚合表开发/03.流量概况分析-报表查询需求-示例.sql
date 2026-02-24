/*
-- dws.actionlog_traffic_aggregate_day
 +-------------+-------+---------------+--------------+-----------------+----------------+-------------+------------------+------------+-----------------+---------------------+-------------------+-----------------+-------------------+
| session_id  | guid  | member_level  | device_type  | enter_province  | exit_province  | enter_page  | enter_page_type  | exit_page  | exit_page_type  | session_start_time  | session_end_time  | session_amt_pv  | session_amt_time  |
+-------------+-------+---------------+--------------+-----------------+----------------+-------------+------------------+------------+-----------------+---------------------+-------------------+-----------------+-------------------+
| s001        | 1     | 2             | mi6          | a               | b              | /p01        | 详情页              | /p08       | 活动页             | 0000001000          | 0000019000        | 3               | 8000.0            |
| s002        | 2     | 3             | mi6          | b               | b              | /p01        | 详情页              | /p01       | 详情页             | 0000001000          | 0000020000        | 3               | 19000.0           |
| s003        | 1     | 2             | mi6          | a               | a              | /p09        | 类目页              | /p12       | 抢购页             | 0000051000          | 0000053000        | 1               | 2000.0            |
+-------------+-------+---------------+--------------+-----------------+----------------+-------------+------------------+------------+-----------------+---------------------+-------------------+-----------------+-------------------+
 */

--
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
    from dws.actionlog_traffic_aggregate_day
where dt='20241101'
group by device_type,member_level
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
--   省	uv总数	访问时长>30分钟的uv数	访问时长>60分钟的uv数	访问时长>120分钟的uv数	会话次数>=2的uv数

with tmp as (select enter_province, guid,
                    sum(session_amt_time) as u_amt_time,   -- 用户的总访问时长
                    count(1) as u_ses_cnt                  -- 用户的总会话数
             from dws.actionlog_traffic_aggregate_day
             where dt = '20241101'
             group by enter_province, guid)

select enter_province,
       count(1)                                               as uv,
       sum(if(u_amt_time > 30, 1, 0))                         as uv_30,
       sum(if(u_amt_time > 60, 1, 0))                         as uv_60,
       sum(if(u_amt_time > 120, 1, 0))                        as uv_120,
       sum(if(u_ses_cnt>=2,1,0))                              as uv_ses_2
from tmp
group by enter_province
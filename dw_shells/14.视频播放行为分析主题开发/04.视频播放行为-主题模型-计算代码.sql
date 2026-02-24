with tmp as (select create_time                                                       as register_time -- 用户注册时间
                  , member_level_id                                                                    -- 会员等级id
                  , gender                                                                             -- 性别
                  , city                                                                               -- 用户注册城市
                  , job                                                                                -- 用户职业
                  , guid                                                                               -- 用户全局标识
                  , url                                                                                -- 页面
                  , properties['videoid']                                             as video_id      -- 视频id
                  , properties['playid']                                              as play_id       -- 视频id
                  , action_time
                  , event_id
                  , sum(if(event_id = 'video_resume', 1, 0))
                        over (partition by properties['playid'] order by action_time) as flag

             from dwd.user_action_log_detail
             where dt = '20241110'
               and event_id in ('video_play', 'video_hb', 'video_pause', 'video_resume', 'video_stop'))


select to_date(register_time) as register_date
     , member_level_id
     , gender
     , city
     , job
     , video_id
     , count(1)                                         as play_count          -- 播放次数
     , count(distinct guid)                             as play_users          -- 播放人数
     , sum(play_time)                                   as play_amt_time       -- 视频播放总时长
     , sum(if(play_time > 5000, 1, 0))                  as play_complete_count -- 视频完播次数
     , count(distinct if(play_time > 5000, guid, null)) as play_complete_users -- 完播人数
     , sum(if(play_time < 2000, 1, 0))                  as play_jump_count     -- 视频跳出次数
     , count(distinct if(play_time < 2000, guid, null)) as play_jump_users     -- 完播跳出人数

from (select register_time,
             member_level_id,
             gender,
             city,
             job,
             guid,
             video_id,
             play_id,
             sum(part_time) as play_time

      from (select register_time,
                   member_level_id,
                   gender,
                   city,
                   job,
                   guid,
                   video_id,
                   play_id,
                   flag,
                   max(action_time) - min(action_time) as part_time
            from tmp
            group by register_time, member_level_id, gender, city, job, guid, video_id, play_id, flag) o1
      group by register_time, member_level_id, gender, city, job, guid, video_id, play_id) o2
group by to_date(register_time), member_level_id, gender, city, job, video_id
/*

+----------------+------------------+---------+-------+------+-----------+-------------+-------------+----------------+----------------------+----------------------+------------------+------------------+
| register_date  | member_level_id  | gender  | city  | job  | video_id  | play_count  | play_users  | play_amt_time  | play_complete_count  | play_complete_users  | play_jump_count  | play_jump_users  |
+----------------+------------------+---------+-------+------+-----------+-------------+-------------+----------------+----------------------+----------------------+------------------+------------------+
| 2024-11-10     | 1                | 0       | 九江    | 化妆师  | 1         | 1           | 1           | 27000          | 1                    | 1                    | 0                | 0                |
| 2024-11-10     | 1                | 1       | 九江    | 化妆师  | 1         | 1           | 1           | 45000          | 1                    | 1                    | 0                | 0                |
| 2024-11-10     | 2                | 0       | 无锡    | 销售员  | 2         | 1           | 1           | 23000          | 1                    | 1                    | 0                | 0                |
| 2024-11-10     | 2                | 1       | 九江    | 化妆师  | 2         | 1           | 1           | 37000          | 1                    | 1                    | 0                | 0                |
+----------------+------------------+---------+-------+------+-----------+-------------+-------------+----------------+----------------------+----------------------+------------------+------------------+



    play_count            bigint,     -- 播放次数
    play_users            string,     -- 播放人数
    play_amt_time         bigint,     -- 视频播放总时长
    play_complete_count   bigint,     -- 视频完播次数
    play_complete_users   bigint,     -- 完播人数
    play_jump_count       bigint,     -- 视频跳出次数
    play_jump_users       bigint      -- 完播跳出人数


+------------------------+------------------+---------+-------+------+-------+-----------+----------+------------+
|     register_time      | member_level_id  | gender  | city  | job  | guid  | video_id  | play_id  | play_time  |
+------------------------+------------------+---------+-------+------+-------+-----------+----------+------------+
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | 1         | pl003    | 27000   |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | 1         | pl001    | 45000   |
| 2024-11-10 08:32:28.0  | 2                | 0       | 无锡    | 销售员  | 2     | 2         | pl002    | 23000   |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | 2         | pl004    | 37000   |
+------------------------+------------------+---------+-------+------+-------+-----------+----------+------------+


 ------------------------------

 +------------------------+------------------+---------+-------+------+-------+------------+-----------+----------+--------------+---------------+-------+
|     register_time      | member_level_id  | gender  | city  | job  | guid  |    url     | video_id  | play_id  | action_time  |   event_id    | flag  |
+------------------------+------------------+---------+-------+------+-------+------------+-----------+----------+--------------+---------------+-------+
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl001    | 20000        | video_play    | 0     |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl001    | 30000        | video_hb      | 0     |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl001    | 40000        | video_hb      | 0     |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl001    | 50000        | video_pause   | 0     |

| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl001    | 80000        | video_resume  | 1     |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl001    | 85000        | video_hb      | 1     |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl001    | 90000        | video_hb      | 1     |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl001    | 95000        | video_stop    | 1     |


| 2024-11-10 08:32:28.0  | 2                | 0       | 无锡    | 销售员  | 2     | /page/w/   | 2         | pl002    | 10000        | video_play    | 0     |
| 2024-11-10 08:32:28.0  | 2                | 0       | 无锡    | 销售员  | 2     | /page/w/   | 2         | pl002    | 15000        | video_hb      | 0     |
| 2024-11-10 08:32:28.0  | 2                | 0       | 无锡    | 销售员  | 2     | /page/w/   | 2         | pl002    | 20000        | video_hb      | 0     |
| 2024-11-10 08:32:28.0  | 2                | 0       | 无锡    | 销售员  | 2     | /page/w/   | 2         | pl002    | 25000        | video_hb      | 0     |
| 2024-11-10 08:32:28.0  | 2                | 0       | 无锡    | 销售员  | 2     | /page/w//  | 2         | pl002    | 30000        | video_hb      | 0     |
| 2024-11-10 08:32:28.0  | 2                | 0       | 无锡    | 销售员  | 2     | /page/w/   | 2         | pl002    | 33000        | video_stop    | 0     |


| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl003    | 210000       | video_play    | 0     |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl003    | 215000       | video_hb      | 0     |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl003    | 220000       | video_hb      | 0     |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl003    | 222000       | video_pause   | 0     |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl003    | 315000       | video_resume  | 1     |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl003    | 320000       | video_hb      | 1     |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl003    | 325000       | video_hb      | 1     |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | 1         | pl003    | 330000       | video_stop    | 1     |

| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | 2         | pl004    | 310000       | video_play    | 0     |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | 2         | pl004    | 315000       | video_hb      | 0     |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | 2         | pl004    | 320000       | video_hb      | 0     |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | 2         | pl004    | 322000       | video_pause   | 0     |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | 2         | pl004    | 415000       | video_resume  | 1     |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | 2         | pl004    | 420000       | video_hb      | 1     |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | 2         | pl004    | 425000       | video_hb      | 1     |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | 2         | pl004    | 440000       | video_stop    | 1     |
+------------------------+------------------+---------+-------+------+-------+------------+-----------+----------+--------------+---------------+-------+
 */







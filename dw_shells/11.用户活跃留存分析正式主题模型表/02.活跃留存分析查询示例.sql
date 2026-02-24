
-- 注册函数
add jar /root/udf.jar;

-- 指定日期范围内的总活跃天数
create temporary function bm_range_cardinality as 'top.doe.hive.dataware.active_retain.BitmapRangeCardinality';

-- 指定日期范围内的最大连续活跃天数
create temporary function bm_range_max_continue as 'top.doe.hive.dataware.active_retain.BitmapRangeMaxContinuousDays';


-- 指定日期是否活跃
create temporary function bm_some_day_active as 'top.doe.hive.dataware.active_retain.BitmapSomeDayActive';


/*
+--------------------+----------------+-----------+------------------------+---------+----------------------------------------------------+
| t.member_level_id  | t.source_type  | t.gender  |     t.create_time      | t.guid  |                   t.active_days                    |
+--------------------+----------------+-----------+------------------------+---------+----------------------------------------------------+
| 1                  | 2              | 1         | 2024-11-01 11:25:20.0  | 1       | �<N=N>N@NANBNCN                                    |
| 1                  | 2              | 0         | 2024-11-01 11:25:20.0  | 2       | �<N=N?N@NANCN                                      |
| 3                  | 2              | 0         | 2024-11-02 11:25:20.0  | 3       | �=N>N?NBN                                          |
| 3                  | 2              | 1         | 2024-11-02 11:25:20.0  | 4       | �=N?N@NAN                                          |
| 1                  | 2              | 1         | 2024-11-08 16:35:40.0  | 5       | �CN                                                |
+--------------------+----------------+-----------+------------------------+---------+----------------------------------------------------+
*/

-- 查询 11月份内各用户的最大连续活跃天数
select
    guid,
    bm_range_max_continue(active_days,'2024-11-01','2024-11-08') as max_continue_days
from dws.user_active_retain_bitmap
where dt='20241108';



-- 查询11月份注册的新用户的周留存人数 （注册后的一周内，有任意一天活跃）
select
  guid
from dws.user_active_retain_bitmap
where dt='20241108'  and   substr(create_time,0,7)='2024-11'  and bm_range_max_continue(active_days, date_add(to_date(create_time),1),date_add(to_date(create_time),7))>=1

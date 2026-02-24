--  就是说在主题建模的时候.有些指标依赖于非常细的粒度才能进行计算出初始值,然后一步步缩小粒度,进行聚合.
-- 但也有些指标需要在相对于细粒度再粗一点才能得到,然后一步步粒度变粗,所以在计算过程中需要从细粒度一步步聚合到粗粒度,这就是为何不直接粗粒度计算的原因,例如跳出会话数,
-- 必须在会话id维度下进行计算,如果没有这个维度,根本没法计算跳出会话术,会话都不存在了,如果粒度非常细的话,
-- 比如包含了flag这个维度,那么计算该会话是否跳出的一个评判标准又无法实现(必须在一个完整的会话中根据pv数计算是否跳出,如果是会话的一部分的pv数,那么结果是不准确的)
-- 为了解决后台唤醒的情景,使用flag对一个会话中的多端打标记,分别计算每段的时间,完美解决时间不好计算的问题
/*
省,手机型号,  guid, level  event_id,      url,session_id,action_time
a,   mi6,    1  ,   2    app_launch,     \N    s001    ,  t1       0     0
a,   mi6,    1  ,   2    page_view,      /p1   s001    ,  t2       0     0
a,   mi6,    1  ,   2    page_view,      /p2   s001    ,  t3       0     0
a,   mi6,    1  ,   2    item_share,     /p2   s001    ,  t4       0     0
a,   mi6,    1  ,   2    push_back,      /p2   s001    ,  t5       0     0

a,   mi6,    1  ,   2    wakeed_up,      /p2   s001    ,  t15      1     1
a,   mi6,    1  ,   2    page_view,      /p3   s001    ,  t16      0     1
a,   mi6,    1  ,   2    app_close,      /p3   s001    ,  t19      0     1

b,   mi6,    2  ,   3    app_launch,     \N    s002    ,  t1
b,   mi6,    2  ,   3    page_view,      /p1   s002    ,  t3
b,   mi6,    2  ,   3    page_view,      /p5   s002    ,  t6
b,   mi6,    2  ,   3    item_share,     /p5   s002    ,  t8
b,   mi6,    2  ,   3    page_view,      /p2   s002    ,  t10
b,   mi6,    2  ,   3    app_close,      /p2   s002    ,  t20


a,   mi6,    1  ,   2    app_launch,     \N    s003    ,  t51
a,   mi6,    1  ,   2    page_view,      /p1   s003    ,  t52
a,   mi6,    1  ,   2    app_close,      /p1   s003    ,  t53

--
province,device_type,level, pv数, uv数,访问总时长，会话总数,跳出会话数


province,device_type,level, session_id, guid,   pv数, 访问总时长
   江西     mi6         2      s001       1      6        12
                              s002       2      20       28

province,device_type,level, session_id, guid, flag,  pv数, 访问总时长
   江西     mi6         2      s001       1     1       6        12
                              s001       1     2       20       28
                              s002       2     1       20       28

*/

-- 注册函数
create temporary function bm_intagg as 'top.doe.hive.dataware.Bigint2BitmapAggFunction';
create temporary function bm_count as 'top.doe.hive.dataware.BitmapCountFunction';
create temporary function bm_union as 'top.doe.hive.dataware.BitmapUnionAggFunction';
create temporary function bm_show as 'top.doe.hive.dataware.BitmapShowFunction';

with tmp as (select device_type,
                    app_version,
                    gps_province,
                    gps_city,
                    gps_region,
                    gender,
                    member_level_id,
                    page_type,
                    event_id,
                    session_id,  --  度量,维度
                    guid,        --  度量,维度
                    action_time, --  度量
                    sum(if(event_id = 'wake_up', 1, 0)) over (partition by guid,session_id order by action_time) as flag
             from dwd.user_action_log_detail
             where dt = '20241101')

select device_type,
       app_version,
       gps_province,
       gps_city,
       gps_region,
       gender,
       member_level_id,
       page_type,
       sum(pv)                    as pv,
       sum(session_timelong)         timelong_amt,
       sum(is_jump)               as jump_session_amt,
       count(distinct session_id) as session_amt,
       bm_intagg(guid)            as uv_bitmap -- 使用bitmap的原因就是为了让分析师进行较粗位置粒度聚合时解决缓慢变化维度地区的问题,所以要编写自定义函数,以便于进行后期的计算
from (select device_type,
             app_version,
             gps_province,
             gps_city,
             gps_region,
             gender,
             member_level_id,
             page_type,
             guid,
             session_id,
             sum(pv)                as pv,
             sum(timelong_part)        session_timelong,
             if(sum(pv) >= 2, 0, 1) as is_jump
      from (select device_type,
                   app_version,
                   gps_province,
                   gps_city,
                   gps_region,
                   gender,
                   member_level_id,
                   page_type,
                   guid,
                   session_id,
                   flag,
                   count(if(event_id = 'page_view', 1, null)) as pv,
                   max(action_time) - min(action_time)        as timelong_part #需要注意的点是以flag作为维度的,持续时间只是一部分,需要减去flag维度进行聚合
            from tmp
            group by device_type,
                     app_version,
                     gps_province,
                     gps_city,
                     gps_region,
                     gender,
                     member_level_id,
                     page_type,
                     guid,
                     session_id,
                     flag) o1
      group by device_type,
               app_version,
               gps_province,
               gps_city,
               gps_region,
               gender,
               member_level_id,
               page_type,
               guid,
               session_id) o2

group by device_type,
         app_version,
         gps_province,
         gps_city,
         gps_region,
         gender,
         member_level_id,
         page_type
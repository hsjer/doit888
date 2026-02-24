/*

-- 行为日志dwd层明细表，样例数据如下

省,手机型号,    页面标识, 页面类型  guid,  level  event_id,      session_id,action_time
a,   mi6,       \N       \N      1  ,   2    app_launch,     s001    ,  t1
a,   mi6,       /p01     详情页   1  ,   2    page_view,      s001    ,  t2
b,   mi6,       /p03     详情页   1  ,   2    page_view,      s001    ,  t3
b,   mi6,       /p02     详情页   1  ,   2    item_share,     s001    ,  t4
b,   mi6,       /p04     详情页   1  ,   2    push_back,      s001    ,  t5

b,   mi6,       /p01     详情页   1  ,   2    wakeed_up,      s001    ,  t15
b,   mi6,       /p06     活动页   1  ,   2    page_view,      s001    ,  t16
b,   mi6,       /p08     活动页   1  ,   2    app_close,      s001    ,  t19

b,   mi6,       \N         \N     2  ,   3    app_launch,    s002    ,  t1
b,   mi6,       /p01     详情页   2  ,   3    page_view,      s002    ,  t3
b,   mi6,       /p01     详情页   2  ,   3    page_view,      s002    ,  t6
b,   mi6,       /p01     详情页   2  ,   3    item_share,     s002    ,  t8
b,   mi6,       /p01     详情页   2  ,   3    page_view,      s002    ,  t10
b,   mi6,       /p01     详情页   2  ,   3    app_close,      s002    ,  t20

a,   mi6,       \N         \N     1  ,   2    app_launch,     s003    ,  t51
a,   mi6,       /p09     类目页   1  ,   2    page_view,      s003    ,  t52
a,   mi6,       /p12     抢购页   1  ,   2    app_close,      s003    ,  t53


 */

-- 测试数据建表
create table tmp.log_detail
(
    province     string,
    device_type  string,
    page_url     string,
    page_type    string,
    guid         bigint,
    member_level int,
    event_id     string,
    session_id   string,
    action_time  bigint

)
    partitioned by (dt string)
    row format delimited fields terminated by ','
;

-- 导入测试数据
/*
-- 以下数据，放入/root/log_test.txt 文件中
a,mi6,\N,\N,1,2,app_launch,s001,1000
a,mi6,/p01,详情页,1,2,page_view,s001,2000
b,mi6,/p03,详情页,1,2,page_view,s001,3000
b,mi6,/p02,详情页,1,2,item_share,s001,4000
b,mi6,/p04,详情页,1,2,push_back,s001,5000
b,mi6,/p01,详情页,1,2,wakeed_up,s001,15000
b,mi6,/p06,活动页,1,2,page_view,s001,16000
b,mi6,/p08,活动页,1,2,app_close,s001,19000
b,mi6,\N,\N,2,3,app_launch,s002,1000
b,mi6,/p01,详情页,2,3,page_view,s002,3000
b,mi6,/p01,详情页,2,3,page_view,s002,6000
b,mi6,/p01,详情页,2,3,item_share,s002,8000
b,mi6,/p01,详情页,2,3,page_view,s002,10000
b,mi6,/p01,详情页,2,3,app_close,s002,20000
a,mi6,\N,\N,1,2,app_launch,s003,51000
a,mi6,/p09,类目页,1,2,page_view,s003,52000
a,mi6,/p12,抢购页,1,2,app_close,s003,53000
*/
load data local inpath '/root/log_test.txt' into table tmp.log_detail partition (dt = '20241101');


/*
-- 优化后的计算逻辑
---- 分段聚合
                                                                                               入口省 和 结束省                 入口页，结束页
+-------------+-------+-------+---------------+--------------+------------------------------------------------------------+--------------------------------------------------------------------------------------------------+----------+-----------+-----------+
| session_id  | flag  | guid  | member_level  | device_type  |                    province_arr                            |                      page_arr                                                                   | part_pv  | part_min  | part_max  |
+-------------+-------+-------+---------------+--------------+------------------------------------------------------------+-------------------------------------------------------------------------------------------------+----------+-----------+-----------+
| s001        | 0     | 1     | 2             | mi6          | ["1000_a","2000_a","3000_b","4000_b","5000_b"]             | ["2000_/p01:详情页","3000_/p03:详情页","4000_/p02:详情页","5000_/p04:详情页"]                        | 2        | 1000      | 5000      |
| s001        | 1     | 1     | 2             | mi6          | ["15000_b","16000_b","19000_b"]                            | ["15000_/p01:详情页","16000_/p06:活动页","19000_/p08:活动页"]                                       | 1        | 15000     | 19000     |
| s002        | 0     | 2     | 3             | mi6          | ["1000_b","3000_b","6000_b","8000_b","10000_b","20000_b"] | ["3000_/p01:详情页","6000_/p01:详情页","8000_/p01:详情页","10000_/p01:详情页","20000_/p01:详情页"]     | 3        | 1000      | 20000     |
| s003        | 0     | 1     | 2             | mi6          | ["51000_a","52000_a","53000_a"]                           | ["52000_/p09:类目页","53000_/p12:抢购页"]                                                          | 1        | 51000     | 53000     |
+-------------+-------+-------+---------------+--------------+-----------------------------------------------------------+---------------------------------------------------------------------------------------------------+----------+-----------+-----------+


---- 全局聚合
--       入口省         结束省        入口页        结束页        总pv            总时长
s001, min(段-first) ,max(段-last) ,min(段-first) ,max(段-last)  sum(pv-part)    ,  sum(时长-part)
 */

with tmp as (select province
                  , device_type
                  , page_url
                  , page_type
                  , guid
                  , member_level
                  , event_id
                  , session_id
                  , lpad(action_time,10,'0') as action_time -- 测试数据的问题特别处理
                  , sum(if(event_id = 'wakeed_up', 1, 0)) over (partition by session_id order by action_time) as flag
             from tmp.log_detail
             where dt = '20241101')
insert into dws.actionlog_traffic_aggregate_day partition (dt='20241101')
select  session_id, guid, member_level, device_type,
        split(min(province_arr[0]),'_')[1] as enter_province,
        split(max(province_arr[size(province_arr)-1]),'_')[1] as exit_province,
        split(min(page_arr[0]),'_')[1] as enter_page,
        split(min(page_arr[0]),'_')[2] as enter_page_type,
        split(max(page_arr[size(page_arr)-1]),'_')[1] as exit_page,
        split(max(page_arr[size(page_arr)-1]),'_')[2] as exit_page_type,
        min(part_min)  as session_start_time,
        max(part_max)  as session_end_time,
        sum(part_pv)   as session_amt_pv,
        sum(part_max-part_min) as session_amt_time

from (select session_id,
                 flag,
                 guid,
                 member_level,
                 device_type,
                 sort_array(collect_list(concat_ws('_', cast(action_time as string), province)))                      as province_arr,
                 sort_array(collect_list(if(page_url is null, null, concat_ws('_', cast(action_time as string),page_url,page_type)))) as page_arr,
                 sum(if(event_id = 'page_view', 1, 0))                                                                as part_pv,
                 min(action_time)                                                                                     as part_min,
                 max(action_time)                                                                                     as part_max
          from tmp
          group by session_id, flag, guid, member_level, device_type) o
group by session_id, guid, member_level, device_type
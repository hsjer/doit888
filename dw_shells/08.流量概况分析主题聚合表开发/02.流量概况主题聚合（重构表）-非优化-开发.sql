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



-- SESSION_ID, guid, 会员等级, 入口页，入口页类型,起始省市区, 起始时间, 结束时间, 退出页, 退出页类型,  结束省市区, 手机型号  || pv数,会话时长

-- 先留下有页面的数据
-- SESSION_ID, guid, 会员等级, 设备类型,入口页，入口页类型,退出页,退出页类型
with tmp1 as (select session_id,
                     guid,
                     member_level,
                     device_type,
                     enter_page_url,
                     enter_page_type,
                     exit_page_url,
                     exit_page_type
              from (select *
                         , first_value(page_url) over (partition by session_id order by action_time)                                               as enter_page_url
                         , first_value(page_type)
                                       over (partition by session_id order by action_time)                                                         as enter_page_type
                         , last_value(page_url)
                                      over (partition by session_id order by action_time rows between unbounded preceding and unbounded following) as exit_page_url
                         , last_value(page_type)
                                      over (partition by session_id order by action_time rows between unbounded preceding and unbounded following) as exit_page_type
                    from tmp.log_detail
                    where dt = '20241101'
                      and page_url is not null) o1
              group by session_id, guid, member_level, device_type, enter_page_url, enter_page_type, exit_page_url,
                       exit_page_type)


-- SESSION_ID, 入口省份,退出省份, 起始时间,结束时间 || pv数,会话时长
,tmp2 as (select session_id,
                     enter_province,
                     exit_province,
                     min(part_min_time)                 as session_start_time, -- 整个会话的起始时间
                     max(part_max_time)                 as session_end_time,   -- 整个会话的结束时间
                     sum(part_max_time - part_min_time) as session_amt_time,   -- 整个会话的时长
                     sum(part_pv)                       as session_amt_pv      -- 整个会话的pv数

              from (select session_id,
                           enter_province,
                           exit_province,
                           flag,
                           min(action_time)                           as part_min_time, -- 每一个分段的最早时间
                           max(action_time)                           as part_max_time, -- 每一个分段的最大时间
                           count(if(event_id = 'page_view', 1, null)) as part_pv        -- 每一个分段的pv数
                    from (select session_id,
                                 event_id,
                                 action_time,
                                 first_value(province) over (partition by session_id order by action_time)                                               as enter_province,
                                 last_value(province)
                                            over (partition by session_id order by action_time rows between unbounded preceding and unbounded following) as exit_province,
                                 sum(if(event_id = 'wakeed_up', 1, 0))
                                     over (partition by session_id order by action_time)                                                                 as flag
                          from tmp.log_detail
                          where dt = '20241101') o2
                    group by session_id, enter_province, exit_province, flag) o3
              group by session_id, enter_province, exit_province)

-- 关联出完整结果
select
    tmp1.session_id,         -- 会话 ID
    tmp1.guid,               -- 会话所属用户
    tmp1.member_level,       -- 会员等级
    tmp1.device_type,        -- 设备型号
    tmp1.enter_page_url,     -- 会话起始入口页面
    tmp1.enter_page_type,    -- 会话起始入口页面类型
    tmp1.exit_page_url,      -- 会话结束所在页面
    tmp1.exit_page_type,     --  会话结束所在页面类型
    tmp2.enter_province,     -- 会话起始所在省
    tmp2.exit_province,      -- 会话结束所在省
    tmp2.session_start_time, -- 整个会话的起始时间
    tmp2.session_end_time,   -- 整个会话的结束时间
    tmp2.session_amt_time,   -- 整个会话的时长
    tmp2.session_amt_pv      -- 整个会话的pv数

from tmp1 join tmp2 on tmp1.session_id =  tmp2.session_id
;
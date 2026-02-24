-- 1. 先过滤出指定漏斗模型的路径中各步骤的事件
with tmp as (SELECT member_level_id,
                    gender,
                    city,
                    job,
                    guid,
                    event_id,
                    action_time,
                    properties
             FROM dwd.user_action_log_detail
             where dt = '20241109'
               and (
                 (event_id = 'search' and properties['keyword'] = '手机')
                     or
                 event_id in ('search_res_click', 'add_cart', 'order_submit', 'order_pay')
                 ))

insert into dws.actionlog_funnel_agg_u partition(dt='20241109')
select member_level_id,
       gender,
       city,
       job,
       guid,
       1    as funnel_model_id,
       '搜索购买漏斗' as funnel_model_name,
       '2024-11-09' as range_start_dt,
       '2024-11-09' as range_end_dt,

       case
            when events_str rlike  'search.*?search_res_click.*?add_cart.*?order_submit.*?order_pay'  then 5
            when events_str rlike  'search.*?search_res_click.*?add_cart.*?order_submit'  then 4
            when events_str rlike  'search.*?search_res_click.*?add_cart'  then 3
            when events_str rlike  'search.*?search_res_click'  then 2
            when events_str rlike  'search'  then 1
       end as deepest_reach_step

from (
-- 2. 把每个用户上一步留下来的事件，收集到一起，按时间先后排序，并变成字符串
         select member_level_id,
                gender,
                city,
                job,
                guid,
                concat_ws('_',sort_array(collect_list(concat_ws(':', cast(action_time as string), event_id))) ) as events_str
         from tmp
         group by member_level_id,
                  gender,
                  city,
                  job,
                  guid) o
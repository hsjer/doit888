-- 创建函数
add jar /root/udf.jar;
create temporary function attr_location as 'top.doe.hive.dataware.attribute.LocationAttributionUDTF';
create temporary function attr_first as 'top.doe.hive.dataware.attribute.FirstTouchAttributionUDTF';
create temporary function attr_last as 'top.doe.hive.dataware.attribute.LastTouchAttributionUDTF';
create temporary function attr_linear as 'top.doe.hive.dataware.attribute.LinearAttributionUDTF';
create temporary function attr_decade as 'top.doe.hive.dataware.attribute.TimeDecadeAttributionUDTF';

-- 包含所有策略函数
add jar /root/udf.jar;
create temporary function attr_whole as 'top.doe.hive.dataware.attribute.WholeStrategyAttributionUDTF';

/* ********************************
    数据预计算：把每个用户的 模型相关的事件，收集成 数组，
    并把结果放在一个临时表中
 * *******************************/
drop table if exists tmp.attr_tmp;
create table tmp.attr_tmp
(
    guid            bigint,
    create_time     timestamp,
    member_level_id bigint,
    gender          int,
    city            string,
    job             string,
    events          array<string>
)
stored as orc;


/* ********************************
    数据预计算：把每个用户的 模型相关的事件，收集成 数组，
    并把结果放在一个临时表中
 * *******************************/



-- create view tmp.attr_tmp as
-- 提取所有做了归因模型中目标业务事件的人
with tmp1 as (select guid
              from dwd.user_action_log_detail
              where dt = '20241110'
                and event_id = 'add_cart'
                and url = '/page101/'
                and to_date(create_time) = '2024-11-10'
              group by guid),

/*
+-------+
| guid  |
+-------+
| 1     |
| 2     |
+-------+
*/

     -- 提取模型所关心的用户行为事件
     tmp2 as (select create_time,
                     member_level_id,
                     gender,
                     city,
                     job,
                     guid,
                     url,
                     case
                         when event_id = 'page_load' and properties['flag'] = 'BAIDU' then 'BAIDU'
                         when event_id = 'page_load' and properties['flag'] = 'XHS' then 'XHS'
                         when event_id = 'page_load' and properties['flag'] = 'SITE_MSG' then 'SITE_MSG'
                         when event_id = 'page_load' and properties['flag'] = 'AD_X' then 'AD_X'
                         when event_id = 'add_cart' and url = '/page101/' then 'GOAL'
                         end as attribution -- 包含待归因，也包含目标事件
                      ,
                     action_time
              from dwd.user_action_log_detail
              where dt = '20241110'
                and (
                  (event_id = 'page_load' and properties['flag'] = 'BAIDU')
                      or
                  (event_id = 'page_load' and properties['flag'] = 'XHS')
                      or
                  (event_id = 'page_load' and properties['flag'] = 'SITE_MSG')
                      or
                  (event_id = 'page_load' and properties['flag'] = 'AD_X')
                      or
                  (event_id = 'add_cart' and url = '/page101/')
                  ))

/*
 +------------------------+------------------+---------+-------+------+-------+------------+--------------+--------------+
|      create_time       | member_level_id  | gender  | city  | job  | guid  |    url     | attribution  | action_time  |
+------------------------+------------------+---------+-------+------+-------+------------+--------------+--------------+
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page101/  | BAIDU        | 20000        |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page101/  | XHS          | 40000        |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page101/  | XHS          | 70000        |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page101/  | GOAL         | 95000        |
| 2024-11-10 18:35:26.0  | 1                | 0       | 无锡    | 程序员  | 2     | /page101/  | XHS          | 70000        |
| 2024-11-10 18:35:26.0  | 1                | 0       | 无锡    | 程序员  | 2     | /page101/  | GOAL         | 95000        |
| 2024-11-10 18:35:26.0  | 1                | 0       | 无锡    | 程序员  | 2     | /page101/  | XHS          | 98000        |
| 2024-11-10 18:35:26.0  | 1                | 0       | 无锡    | 程序员  | 2     | /page101/  | GOAL         | 99700        |
| 2024-11-10 18:35:26.0  | 1                | 0       | 无锡    | 程序员  | 2     | /page101/  | XHS          | 99600        |
+------------------------+------------------+---------+-------+------+-------+------------+--------------+--------------+
 */

-- 预计算的结果插入临时表
insert into tmp.attr_tmp
select tmp2.guid,
       tmp2.create_time,
       tmp2.member_level_id,
       tmp2.gender,
       tmp2.city,
       tmp2.job,
       sort_array(collect_list(concat_ws(':', cast(action_time as string), attribution))) as events
from tmp2
         join tmp1 on tmp2.guid = tmp1.guid
group by tmp2.guid, tmp2.create_time, tmp2.member_level_id, tmp2.gender, tmp2.city, tmp2.job;


/*
 +------------+------------------------+-----------------------+--------------+------------+-----------+----------------------------------------------------+
| tmp2.guid  |    tmp2.create_time    | tmp2.member_level_id  | tmp2.gender  | tmp2.city  | tmp2.job  |                       events                       |
+------------+------------------------+-----------------------+--------------+------------+-----------+----------------------------------------------------+
| 1          | 2024-11-10 08:32:28.0  | 1                     | 1            | 九江         | 化妆师       | ["20000:BAIDU","40000:XHS","70000:XHS","95000:GOAL"] |
| 2          | 2024-11-10 18:35:26.0  | 1                     | 0            | 无锡         | 程序员       | ["70000:XHS","95000:GOAL","98000:XHS","99700:GOAL","99600:XHS"] |
+------------+------------------------+-----------------------+--------------+------------+-----------+----------------------------------------------------+

 */


/* ********************************
    归因策略的计算 非优化方式：：
         首次触点归因
         末次触点归因
         时间衰减归因
         线性归因
         位置归因
 * *******************************/


-- 把当天的行为明细， 关联  上面提取的人群，留下所有目标业务事件的用户行为数据
INSERT INTO table dws.actionlog_attribution_agg_u partition (dt = '20241110')
select '速7归因-001'  as attribution_model, -- 归因模型
       '速7新用户购'  as business_goal,     -- 业务目标
       '位置归因策略' as attribution_strategy,
       o.guid,
       o.create_time,
       o.member_level_id,
       o.gender,
       o.city,
       o.job,
       x.goal_time    as goal_happen_time,
       x.to_attribute_event,
       x.contribution as contribution_factor
from tmp.attr_tmp o
         lateral view
         attr_location(events) x as goal_time, to_attribute_event, contribution
UNION ALL
select '速7归因-001'      as attribution_model, -- 归因模型
       '速7新用户购'      as business_goal,     -- 业务目标
       '首次触点归因策略' as attribution_strategy,
       o.guid,
       o.create_time,
       o.member_level_id,
       o.gender,
       o.city,
       o.job,
       x.goal_time        as goal_happen_time,
       x.to_attribute_event,
       x.contribution     as contribution_factor
from tmp.attr_tmp o
         lateral view
         attr_first(events) x as goal_time, to_attribute_event, contribution

UNION ALL
select '速7归因-001'      as attribution_model, -- 归因模型
       '速7新用户购'      as business_goal,     -- 业务目标
       '末次触点归因策略' as attribution_strategy,
       o.guid,
       o.create_time,
       o.member_level_id,
       o.gender,
       o.city,
       o.job,
       x.goal_time        as goal_happen_time,
       x.to_attribute_event,
       x.contribution     as contribution_factor
from tmp.attr_tmp o
         lateral view
         attr_last(events) x as goal_time, to_attribute_event, contribution

UNION ALL
select '速7归因-001'      as attribution_model, -- 归因模型
       '速7新用户购'      as business_goal,     -- 业务目标
       '时间衰减归因策略' as attribution_strategy,
       o.guid,
       o.create_time,
       o.member_level_id,
       o.gender,
       o.city,
       o.job,
       x.goal_time        as goal_happen_time,
       x.to_attribute_event,
       x.contribution     as contribution_factor
from tmp.attr_tmp o
         lateral view
         attr_decade(events) x as goal_time, to_attribute_event, contribution

UNION ALL
select '速7归因-001'  as attribution_model, -- 归因模型
       '速7新用户购'  as business_goal,      -- 业务目标
       '线性归因策略' as attribution_strategy,
       o.guid,
       o.create_time,
       o.member_level_id,
       o.gender,
       o.city,
       o.job,
       x.goal_time    as goal_happen_time,
       x.to_attribute_event,
       x.contribution as contribution_factor
from tmp.attr_tmp o
         lateral view
         attr_linear(events) x as goal_time, to_attribute_event, contribution
;





/* ********************************
    归因策略的计算 优化方式：一次性计算出所有策略的结果
 * *******************************/
INSERT overwrite table dws.actionlog_attribution_agg_u partition (dt = '20241110')
select '速7归因-001'  as attribution_model, -- 归因模型
       '速7新用户购'  as business_goal,      -- 业务目标
       x.attribution_strategy as attribution_strategy,
       o.guid,
       o.create_time,
       o.member_level_id,
       o.gender,
       o.city,
       o.job,
       x.goal_happen_time    as goal_happen_time,
       x.to_attribute_event,
       x.contribution_factor as contribution_factor
from tmp.attr_tmp o
         lateral view
         attr_whole(events) x as attribution_strategy,goal_happen_time, to_attribute_event, contribution_factor
;
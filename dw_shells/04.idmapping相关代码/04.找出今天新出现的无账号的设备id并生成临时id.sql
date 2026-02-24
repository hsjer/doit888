-- 逻辑：
/*
   从T日的日志中找出 没有账号的 device_id
   然后从中剔除掉 存在于  “设备账号绑定权重表” 和 “匿名设备临时id表" 的 （剩下的肯定是新增的匿名设备）
   然后，为剩下的device_id 生成  临时id
 */

with ids as (
    -- “设备账号绑定权重表” 中的所有 device_id
    select device_id
    from dws.device_username_bind
    where dt = '20241101'
    group by device_id

    UNION ALL

    -- “匿名设备临时id表” 中的所有 device_id
    select device_id
    from dim.device_ano_tmpid)

-- 从T日的日志中找出 没有账号的 device_id
,t_ids as (select device_id
           from ods.user_action_log
           where dt = '20241101'
             and username is null
           group by device_id)

insert into dim.device_ano_tmpid
select
    o3.device_id,
    -- 用行号 + 最大id
    o3.rn + o4.max_tmpid as tmpid,
    unix_timestamp() * 1000 as create_time

from (
         -- 从T日日志中出现的匿名device_id ，剔除 掉 历史上已存在的 ,并为它们生成行号
        select t_ids.device_id,
             row_number() over () as rn
      from t_ids
               left join ids on t_ids.device_id = ids.device_id
      where ids.device_id is null) o3
join
     -- 找出临时id表中的最大id
     (select max(tmpid) as max_tmpid
      from dim.device_ano_tmpid) o4
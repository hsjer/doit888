with has_u as (
    select
        *
    from ods.user_action_log
    where dt='20241101'  and username is not null
),
     no_u as (
         select
             *
         from ods.user_action_log
         where dt='20241101'  and username is null
     ),
     u as (
         select
             *
         from dim.ums_member_scd
         where dt='20241101' and start_dt <= dt and end_dt >=dt
     ),
-- 加工绑定表，每个设备取权重最高的账号
     bind as (
         select device_id,user_name
         from (
                  select
                      * ,
                      row_number() over(partition by device_id order by bind_weight desc,last_logintime desc) as rn
                  from device_username_bind
                  where dt='20241101'
              )
         where rn=1
     )

-- 有username
select has_u.*,u.id as guid
from has_u join u  on has_u.username = u.username


UNION ALL


select
    o1.* , u.id as guid
from
    (
        -- 无username ，能关联到绑定表
        select no_u.*,bind.user_name
        from no_u join bind on no_u.device_id = bind.device_id
    ) o1
-- 再关联用户表
        join u
             on o1.user_name = u.username


UNION ALL


select

    o2.*,t.tmpid as guid

from (
         -- 找出匿名设备且没有绑定账号的
         select no_u.*
         from no_u left join bind on no_u.device_id = bind.device_id
         where bind.device_id is null

     ) o2
JOIN dim.device_ano_tmpid t ON o2.device_id= t.device_id


-- 接着将上面的数据拆分成： 关联到tmpid的和关联不到tmpid的

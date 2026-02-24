/*
    创建idmapping后的结果临时表
 */

create table tmp.log_guid (
username     string
,app_id     string
,app_version     string
,release_channel     string
,carrier     string
,net_type     string
,ip     string
,device_id     string
,device_type     string
,resolution     string
,os_name     string
,os_version     string
,latitude     double   -- 纬度
,longitude     double  -- 经度
,event_id     string
,properties     map<string,string>
,action_time     bigint
,session_id     string
,user_id     bigint
,guid     bigint
)
stored as orc;


/*
    后面是生成guid的逻辑
 */

-- 绑定权重表加工：每个设备只取权重最高的账号
with bind as (select device_id, user_name
              from (select device_id,
                           user_name,
                           row_number() over (partition by device_id order by bind_weight desc ,last_logintime desc ) as rn
                    from dws.device_username_bind
                    where dt = '20241101') o1
              where rn = 1),
-- 用户注册信息拉链表：取最新数据
     u as (select username, id
           from dwd.ums_member_scd
           where dt = '20241101'
             and (dt between start_dt and end_dt)),
     log as (select *
             from ods.user_action_log
             where dt = '20241101')

insert overwrite table tmp.log_guid
select log.username
     , log.app_id
     , log.app_version
     , log.release_channel
     , log.carrier
     , log.net_type
     , log.ip
     , log.device_id
     , log.device_type
     , log.resolution
     , log.os_name
     , log.os_version
     , log.latitude
     , log.longitude
     , log.event_id
     , log.properties
     , log.action_time
     , log.session_id
     , u.id               as user_id
     , nvl(u.id, t.tmpid) as guid

from log
         left join bind on log.device_id = bind.device_id
    -- 优先用日志中的username去获取user_id，如果没有采用绑定表的user_name去获取user_id
         left join u on nvl(log.username, bind.user_name) = u.username
         left join dim.device_ano_tmpid t on log.device_id = t.device_id
;
/* ***************************************************
   用户活跃、留存主题分析模型表 DDL创建
 * **************************************************/
drop table if exists dws.user_active_retain_bitmap;
CREATE TABLE dws.user_active_retain_bitmap(
      member_level_id int,   -- 会员等级
      source_type  int,   -- 注册来源
      gender       int,   -- 性别
      create_time  timestamp,   -- 注册时间
      guid         bigint,    --
      active_days  binary       -- 活跃日期的bitmap记录
)
partitioned by (dt string)
stored as orc
tblproperties (
    'orc.compress' = 'snappy'
);


/* ***************************************************
   所需的 自定义函数
 * **************************************************/
-- 注册函数
add jar /root/udf.jar;

-- 输入多个日期，返回bitmap的字节
create temporary function dt2bitmap as 'top.doe.hive.dataware.active_retain.DateArray2BitmapFunction';

-- 输入bitmap，打印其中包含的活跃日期
create temporary function show_dates as 'top.doe.hive.dataware.active_retain.BitmapShowDateFunction';

-- 输入一个bitmap和任意个日期，生成新的bitmap
create temporary function bm_add_dt  as 'top.doe.hive.dataware.active_retain.BitmapAddDateFunction';






/* ***************************************************
   所需的 活跃bitmap模型表 T-1日 测试数据
 * **************************************************/
insert into table dws.user_active_retain_bitmap partition(dt='20241107')
values
(1,2,1,'2024-11-01 11:25:20',1,dt2bitmap('2024-11-01','2024-11-02','2024-11-03','2024-11-05','2024-11-06','2024-11-07'))
,(1,2,0,'2024-11-01 11:25:20',2,dt2bitmap('2024-11-01','2024-11-02','2024-11-04','2024-11-05','2024-11-06'))
,(3,2,0,'2024-11-02 11:25:20',3,dt2bitmap('2024-11-02','2024-11-03','2024-11-04','2024-11-07'))
,(3,2,1,'2024-11-02 11:25:20',4,dt2bitmap('2024-11-02','2024-11-04','2024-11-05','2024-11-06'))
;

/* ***************************************************
   活跃 bitmap模型表 的滚动更新
      逻辑：  T-1日的bitmap模型表  FULL JOIN T日的日活用户
              左边、右边都有，则bitmap添加当天日期
              左边有，右边无，则还是原来的bitmap
              左边无，右边有，则创建一个新的 bitmap并添加当前日期
 * **************************************************/

 -- 提取 bitmap模型表的 T-1日 分区
with bm as (
    select * from dws.user_active_retain_bitmap where dt='20241107'
),
-- 提取 T日 的日活用户
dau as (
    select member_level_id,source_type,gender,create_time,guid
    from    dwd.user_action_log_detail
    where dt='20241108'
    group by member_level_id,source_type,gender,create_time,guid
)

insert into  dws.user_active_retain_bitmap partition (dt='20241108')
select
    nvl(a.member_level_id,b.member_level_id) as member_level_id,
    nvl(a.source_type,b.source_type) as source_type,
    nvl(a.gender,b.gender) as gender,
    nvl(a.create_time,b.create_time) as create_time,
    nvl(a.guid,b.guid) as guid,
    case
        when a.guid is not null and b.guid is not null then bm_add_dt(a.active_days,'2024-11-08')
        when a.guid is not null and b.guid is     null then a.active_days
        when a.guid is     null then dt2bitmap('2024-11-08')
    end as active_days
from bm a full join dau b
    on a.guid = b.guid




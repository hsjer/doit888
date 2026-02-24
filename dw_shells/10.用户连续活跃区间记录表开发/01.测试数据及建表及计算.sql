
/* ********************************************
   行为日志明细表，测试数据
 *********************************************/
--假如，T日(8号）的日志明细  dwd.user_action_log_detail
 --会员等级,注册来源,性别,注册时间,guid
 1,2,1,'2024-11-01 11:25:20',1
 1,2,0,'2024-11-01 11:25:20',2
 1,2,1,'2024-11-08 16:35:40',5

-- 导入测试数据到日志明细表
alter table dwd.user_action_log_detail drop partition(dt='20241108');


insert into table dwd.user_action_log_detail partition(dt='20241108')
(member_level_id,source_type,gender,create_time,guid)
values
    (1,2,1,'2024-11-01 11:25:20',1),
    (1,2,0,'2024-11-01 11:25:20',2),
    (1,2,1,'2024-11-08 16:35:40',5);



/* ********************************************
   连续活跃区间记录表 DDL 建表
 *********************************************/
-- T-1 日的连续活跃区间记录表(dws.user_active_range)，如上文
+-----------------+----------------+-----------+------------------------+---------+----------------+--------------+-----------+
| t.member_level  | t.source_type  | t.gender  |     t.create_time      | t.guid  | t.range_start  | t.range_end  |   t.dt    |
+-----------------+----------------+-----------+------------------------+---------+----------------+--------------+-----------+
| 1               | 2              | 1         | 2024-11-01 11:25:20.0  | 1       | 2024-11-01     | 2024-11-05   | 20241106  |
| 1               | 2              | 1         | 2024-11-01 11:25:20.0  | 1       | 2024-11-07     | 9999-12-31   | 20241106  |
| 1               | 2              | 0         | 2024-11-01 11:25:20.0  | 2       | 2024-11-01     | 2024-11-03   | 20241106  |
| 1               | 2              | 0         | 2024-11-01 11:25:20.0  | 2       | 2024-11-05     | 2024-11-05   | 20241106  |
| 3               | 2              | 0         | 2024-11-02 11:25:20.0  | 3       | 2024-11-02     | 2024-11-05   | 20241106  |
| 3               | 2              | 0         | 2024-11-02 11:25:20.0  | 3       | 2024-11-07     | 9999-12-31   | 20241106  |
| 3               | 2              | 1         | 2024-11-02 11:25:20.0  | 4       | 2024-11-02     | 2024-11-02   | 20241106  |
| 3               | 2              | 1         | 2024-11-02 11:25:20.0  | 4       | 2024-11-04     | 2024-11-04   | 20241106  |
+-----------------+----------------+-----------+------------------------+---------+----------------+--------------+-----------+

/* ************************************************* */
drop table if exists dws.user_active_range;
CREATE TABLE dws.user_active_range(
                                      member_level_id int,   -- 会员等级
                                      source_type  int,   -- 注册来源
                                      gender       int,   -- 性别
                                      create_time  timestamp,   -- 注册时间
                                      guid         bigint,    --
                                      range_start  string,    -- 连续区间起始
                                      range_end    string     -- 连续区间结束
)
    partitioned by (dt string)
    row format delimited fields terminated by ','
;


/* ********************************************
   用户连续活跃区间记录表，测试数据
 *********************************************/
-- 导入测试数据
load data local inpath '/root/range.txt' into table dws.user_active_range partition(dt='20241107')



-- 测试数据, 放入一个文件中  /root/range.txt
1,2,1,2024-11-01 11:25:20,1,2024-11-01,2024-11-05
1,2,1,2024-11-01 11:25:20,1,2024-11-07,9999-12-31
1,2,0,2024-11-01 11:25:20,2,2024-11-01,2024-11-03
1,2,0,2024-11-01 11:25:20,2,2024-11-05,2024-11-05
3,2,0,2024-11-02 11:25:20,3,2024-11-02,2024-11-05
3,2,0,2024-11-02 11:25:20,3,2024-11-07,9999-12-31
3,2,1,2024-11-02 11:25:20,4,2024-11-02,2024-11-02
3,2,1,2024-11-02 11:25:20,4,2024-11-04,2024-11-04




/* ********************************************
   更新计算出T日的连续活跃区间记录表
 *********************************************/

-- 提取T日的日活用户
with dau as (
    select  member_level_id,source_type,gender,create_time,guid
    from  dwd.user_action_log_detail
    where dt='20241108'
    group by member_level_id,source_type,gender,create_time,guid
)
-- 提取T-1的活跃区间表分区
,active_range as (
   select * from dws.user_active_range where dt='20241107'
)


-- 取出连续活跃区间表中所有非9999的记录
select
     member_level_id   -- 会员等级
     ,source_type     -- 注册来源
     ,gender          -- 性别
     ,create_time     -- 注册时间
     ,guid            --
     ,range_start
     ,range_end
from active_range
where range_end != '9999-12-31'

UNION ALL

-- 计算逻辑： full join后判断取值
select

     nvl(a.member_level_id,b.member_level_id) as  member_level_id   -- 会员等级
     ,nvl(a.source_type,b.source_type) as  source_type     -- 注册来源
     ,nvl(a.gender,b.gender) as  gender          -- 性别
     ,nvl(a.create_time,b.create_time) as  create_time     -- 注册时间
     ,nvl(a.guid,b.guid) as guid            --
     ,nvl(range_start,'2024-11-08')     -- 连续区间起始 (左边有就取左边，左没有说明是今天新出现的用户，则start取今天）
     ,case
         when a.range_end is not null and b.guid is not null then a.range_end
         when a.range_end is not null and b.guid is     null then '2024-11-07'
         when a.range_end is null  then '9999-12-31'
      end as    range_end       -- 连续区间结束

from (
       SELECT  *
       FROM active_range
       where range_end = '9999-12-31'
) a  full join dau b on a. guid = b.guid
;

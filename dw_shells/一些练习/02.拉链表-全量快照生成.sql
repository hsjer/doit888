-- 有如下全量快照表及数据
create table tmp.excer_scd(
    oid int,
    amt int,
    status string,
    modify_time string
)
partitioned by (dt string)
stored as orc;


/*
oid	amt	status	modify_time	partition_dt
1	100	待支付	2024/10/29	2024/10/29
2	200	待发货	2024/10/29	2024/10/29
3	150	待支付	2024/10/29	2024/10/29

1	100	待发货	2024/10/30	2024/10/30
2	200	待发货	2024/10/29	2024/10/30
3	150	待支付	2024/10/29	2024/10/30
4	200	待支付	2024/10/30	2024/10/30

1	100	待确认	2024/10/31	2024/10/31
2	200	已确认	2024/10/31	2024/10/31
3	150	待支付	2024/10/29	2024/10/31
4	200	待发货	2024/10/31	2024/10/31
5	180	待支付	2024/10/31	2024/10/31
*/

insert into table tmp.excer_scd partition(dt) values
(1,100,'待支付','2024/10/29','2024/10/29')
                                                   ,(2,200,'待发货','2024/10/29','2024/10/29')
                                                   ,(3,150,'待支付','2024/10/29','2024/10/29')
                                                   ,(1,100,'待发货','2024/10/30','2024/10/30')
                                                   ,(2,200,'待发货','2024/10/29','2024/10/30')
                                                   ,(3,150,'待支付','2024/10/29','2024/10/30')
                                                   ,(4,200,'待支付','2024/10/30','2024/10/30')
                                                   ,(1,100,'待确认','2024/10/31','2024/10/31')
                                                   ,(2,200,'已确认','2024/10/31','2024/10/31')
                                                   ,(3,150,'待支付','2024/10/29','2024/10/31')
                                                   ,(4,200,'待发货','2024/10/31','2024/10/31')
                                                   ,(5,180,'待支付','2024/10/31','2024/10/31');


-- 假如现在的日期就是2024/10/31   请为上面的表和数据，生成一个拉链表
-- 先去重：
with tmp as (select oid, amt, status, modify_time
             from tmp.excer_scd
             group by oid, amt, status, modify_time)
SELECT  oid,amt,status,modify_time,
        start_dt,
        if(change_time is not null ,date_sub(replace(change_time,'/','-'),1),'9999-12-31') as end_dt
        -- 需要注意的是lead函数在最后一条数据会产生一条空值,需要将其给过滤掉
FROM (select oid,
                 amt,
                 status,
                 modify_time,
             replace(modify_time,'/','-')                                          as start_dt,
                 lead(modify_time, 1) over (partition by oid order by modify_time) as change_time  -- 取下一条数据的变更时间作为end_time,
                 -- 以订单划分窗口,时间升序,使用lead函数获取下一条数据变更时间作为本条数据的end_time.
          from tmp) o

-- 然后，基于生成的拉链表，查询： 2024-10-30日时的待支付订单总数、订单总额、及所涉及的用户数

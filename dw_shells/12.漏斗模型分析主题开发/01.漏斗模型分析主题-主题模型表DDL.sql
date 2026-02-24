
-- 漏斗分析 -  主题模型表  创建
drop table if exists dws.actionlog_funnel_agg_u;
create table dws.actionlog_funnel_agg_u(
    member_level_id    int,     -- 用户的会员等级id
    gender             int,     -- 用户的性别
    city               string,  -- 用户所在城市
    job                string,  -- 用户的职业
    guid               bigint,  -- 用户标识
    funnel_model_id    int,     -- 漏斗模型id
    funnel_model_name  string,  -- 漏斗模型名称
    range_start_dt     string,  -- 漏斗的统计时间跨度起始日期
    range_end_dt        string,  -- 漏斗的统计时间跨度结束日期
    deepest_reach_step int      -- 最深触达步骤号
)
partitioned by (dt string)
stored as orc
tblproperties (
    'orc.compress' = 'snappy'
    )
;
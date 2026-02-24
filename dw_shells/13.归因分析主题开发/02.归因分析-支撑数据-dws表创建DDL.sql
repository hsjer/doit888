create table dws.actionlog_attribution_agg_u(
    attribution_model    string,  -- 归因模型id
    business_goal        string,  -- 业务目标
    attribution_strategy string,  -- 归因策略
    guid                 bigint,  -- 用户id
    create_time          timestamp, -- 用户的注册时间
    member_level_id      int ,    -- 用户等级
    gender               int,     -- 用户性别
    city                 string,  -- 用户注册城市
    job                  string,  -- 用户注册的职业
    goal_happen_time     bigint,  -- 业务目标发生时间
    to_attribute_event   string,  -- 待归因事件
    contribution_factor  double   -- 贡献度

)
partitioned by (dt string)
stored as orc
tblproperties (
    'orc.compress' = 'snappy'
    );
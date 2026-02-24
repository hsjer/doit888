-- 漏斗模型分析，主题dws表数据如下：
+------+-----------+---------+--------+---------+--------------------+----------------------+-------------------+-----------------+----------+-----------+
| t.me | t.gender  | t.city  | t.job  | t.guid  | t.funnel_model_id  | t.funnel_model_name  | t.range_start_dt  | t.range_end_dt  | t._step  |   t.dt    |
+------+-----------+---------+--------+---------+--------------------+----------------------+-------------------+-----------------+----------+-----------+
| 1    | 1         | 九江    | 化妆师 | 1       | 1                  | 搜索购买漏斗         | 2024-11-09        | 2024-11-09      | 5        | 20241109  |
| 1    | 1         | 九江    | 美发师 | 2       | 1                  | 搜索购买漏斗         | 2024-11-09        | 2024-11-09      | 4        | 20241109  |
| 1    | 1         | 南昌    | 化妆师 | 4       | 1                  | 搜索购买漏斗         | 2024-11-09        | 2024-11-09      | 2        | 20241109  |
| 1    | 1         | 南昌    | 美发师 | 3       | 1                  | 搜索购买漏斗         | 2024-11-09        | 2024-11-09      | 3        | 20241109  |
+------+-----------+---------+--------+---------+--------------------+----------------------+-------------------+-----------------+----------+-----------+



 -- 漏斗模型1，20241109号统计跨度，各职业用户的，每个步骤的触达人数，和每个步骤的相对转化率，绝对转化率

select
    job,
    sum(if(deepest_reach_step >= 1,1,0)) as step1_u,
    sum(if(deepest_reach_step >= 2,1,0)) as step2_u,
    round(sum(if(deepest_reach_step >= 2,1,0))/sum(if(deepest_reach_step >= 1,1,0)),2) as step2_step1_ratio,
    round(sum(if(deepest_reach_step >= 2,1,0))/sum(if(deepest_reach_step >= 1,1,0)),2) as step2_abs_ratio,
    sum(if(deepest_reach_step >= 3,1,0)) as step3_u,
    round(sum(if(deepest_reach_step >= 3,1,0))/sum(if(deepest_reach_step >= 2,1,0)),2) as step3_step2_ratio,
    round(sum(if(deepest_reach_step >= 3,1,0))/sum(if(deepest_reach_step >= 1,1,0)),2) as step3_abs_ratio,
    sum(if(deepest_reach_step >= 4,1,0)) as step4_u,
    round(sum(if(deepest_reach_step >= 4,1,0))/sum(if(deepest_reach_step >= 3,1,0)),2) as step4_step3_ratio,
    round(sum(if(deepest_reach_step >= 4,1,0))/sum(if(deepest_reach_step >= 1,1,0)),2) as step4_abs_ratio,
    sum(if(deepest_reach_step >= 5,1,0)) as step5_u,
    round(sum(if(deepest_reach_step >= 5,1,0))/sum(if(deepest_reach_step >= 4,1,0)),2) as step5_step4_ratio,
    round(sum(if(deepest_reach_step >= 5,1,0))/sum(if(deepest_reach_step >= 1,1,0)),2) as step5_abs_ratio


from dws.actionlog_funnel_agg_u
where dt='20241109'  and range_start_dt='2024-11-09' and range_end_dt='2024-11-09' and funnel_model_id=1
group by job
;


+------+----------+----------+--------------------+------------------+----------+--------------------+------------------+----------+--------------------+------------------+----------+--------------------+------------------+
| job  | step1_u  | step2_u  | step2_step1_ratio  | step2_abs_ratio  | step3_u  | step3_step2_ratio  | step3_abs_ratio  | step4_u  | step4_step3_ratio  | step4_abs_ratio  | step5_u  | step5_step4_ratio  | step5_abs_ratio  |
+------+----------+----------+--------------------+------------------+----------+--------------------+------------------+----------+--------------------+------------------+----------+--------------------+------------------+
| 化妆师  | 2        | 2        | 1.0                | 1.0              | 1        | 0.5                | 0.5              | 1        | 1.0                | 0.5              | 1        | 1.0                | 0.5              |
| 美发师  | 2        | 2        | 1.0                | 1.0              | 2        | 1.0                | 1.0              | 1        | 0.5                | 0.5              | 0        | 0.0                | 0.0              |
+------+----------+----------+--------------------+------------------+----------+--------------------+------------------+----------+--------------------+------------------+----------+--------------------+------------------+
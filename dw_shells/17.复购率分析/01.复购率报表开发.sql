
CREATE TEMPORARY VIEW tmp.fugou_view  AS
with it as (

    SELECT
       order_id           -- 订单id
       ,product_id        -- 商品id
       ,product_brand     -- 品牌
    FROM dwd.oms_order_item_scd
    WHERE dt='20241110' and ('2024-11-10' BETWEEN start_dt and end_dt)

)
, od as (
    SELECT
       order_id           -- 订单id
       ,member_id         -- 用户id
       ,to_date(create_time) as order_date  -- 订单日期
    FROM dwd.oms_order_scd
    WHERE dt='20241110' and ('2024-11-10' BETWEEN start_dt and end_dt) and year(create_time)='2024'
)
,u as (
    SELECT
       member_id   -- 用户id
       ,gender     -- 性别
    FROM dwd.ums_member_scd
    WHERE dt='20241110' and ('2024-11-10' BETWEEN start_dt and end_dt)
)


SELECT
    it.order_id           -- 订单id
     ,it.product_id         -- 商品id
     ,it.product_brand      -- 品牌
     ,od.member_id          -- 用户id
     ,od.order_date         -- 订单日期
     ,u.gender              -- 性别
     ,weekofyear(order_date) AS week_of_year,  -- 所属周
     ,month(order_date) AS month_of_year,      -- 所属月
     ,quarter(order_date) AS quarter_of_year;  -- 所属季
,year(order_date) AS  of_year;               -- 所属年
FROM it JOIN od ON it.order_id = od.order_id
        JOIN u  ON od.member_id = u.member_id
;




/*
    最终取结果
*/
SELECT

    xw.product_brand           --  品牌
     ,xw.gender                  --  性别
     ,xw.week_of_year            --  所属周
     ,xw.week_fg_ratio           --  周复购率

     ,xw.month_of_year           --  月份
     ,xm.month_fg_ratio          --  月复购率

     ,xw.quarter_of_year         --  季度
     ,xq.quarter_fg_ratio        --  季复购率

     ,xw.of_year                 --  年
     ,xy.year_fg_ratio           --  年复购率

FROM (

         -- 2.计算周复购率
         SELECT
             product_brand
              ,gender
              ,week_of_year
              ,month_of_year
              ,quarter_of_year
              ,of_year
              ,sum(if(week_buy_times>=2,1,0) )/count(1) as  week_fg_ratio   -- 周复购率

         from (

                  -- 1.先计算出各维度下，每用户每周的购买次数
                  SELECT
                      member_id
                       ,product_brand
                       ,gender
                       ,week_of_year
                       ,month_of_year
                       ,quarter_of_year
                       ,of_year
                       ,count(distinct order_id) as week_buy_times   -- 周购买次数
                  FROM  tmp.fugou_view
                  GROUP BY
                      member_id
                         ,product_brand
                         ,gender
                         ,of_year
                         ,quarter_of_year
                         ,month_of_year
                         ,week_of_year
              ) tw
         group by
             product_brand
                ,gender
                ,week_of_year
                ,month_of_year
                ,quarter_of_year
                ,of_year
     ) xw

         JOIN

     (

         -- 2.再计算月复购率
         SELECT
             product_brand
              ,gender
              ,month_of_year
              ,quarter_of_year
              ,of_year
              ,sum(if(month_buy_times>=2,1,0) )/count(1) as  month_fg_ratio   -- 月复购率

         FROM (

                  -- 1.先计算出各维度下，每用户每月的购买次数
                  SELECT
                      member_id
                       ,product_brand
                       ,gender
                       ,of_year
                       ,quarter_of_year
                       ,month_of_year
                       ,count(distinct order_id) as month_buy_times   -- 月购买次数
                  FROM  tmp.fugou_view
                  GROUP BY
                      member_id
                         ,product_brand
                         ,gender
                         ,of_year
                         ,quarter_of_year
                         ,month_of_year
              ) tm
         GROUP BY
             product_brand
                ,gender
                ,month_of_year
                ,quarter_of_year
                ,of_year
     ) xm

     ON xw.month_of_year = xm.month_of_year


         JOIN (

    -- 2.再计算季复购率
    SELECT
        product_brand
         ,gender
         ,quarter_of_year
         ,of_year
         ,sum(if(quarter_buy_times>=2,1,0) )/count(1) as  quarter_fg_ratio   -- 季复购率

    FROM (

             -- 1.先计算出各维度下，每用户每季度的购买次数
             SELECT
                 member_id
                  ,product_brand
                  ,gender
                  ,of_year
                  ,quarter_of_year
                  ,count(distinct order_id) as quarter_buy_times   -- 季度购买次数
             FROM  tmp.fugou_view
             GROUP BY
                 member_id
                    ,product_brand
                    ,gender
                    ,of_year
                    ,quarter_of_year
         ) tq
    GROUP BY
        product_brand
           ,gender
           ,quarter_of_year
           ,of_year

) xq

              ON xw.quarter_of_year = xq.quarter_of_year


         JOIN (

    -- 2.再计算年复购率
    SELECT
        product_brand
         ,gender
         ,of_year
         ,sum(if(year_buy_times>=2,1,0) )/count(1) as  year_fg_ratio   -- 年复购率

    FROM (

             -- 1.先计算出各维度下，每用户的年购买次数
             SELECT
                 member_id
                  ,product_brand
                  ,gender
                  ,of_year
                  ,count(distinct order_id) as year_buy_times   -- 年购买次数
             FROM  tmp.fugou_view
             GROUP BY
                 member_id
                    ,product_brand
                    ,gender
                    ,of_year
         ) ty
    GROUP BY
        product_brand
           ,gender
           ,of_year

) xy

              ON xw.of_year = xy.of_year
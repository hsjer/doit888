/*
    购物车拉链表创建
 */
CREATE TABLE dwd.oms_cart_item_scd
(
    id                  bigint,
    product_id          bigint,
    product_sku_id      bigint,
    member_id           bigint,
    quantity            int,
    price               decimal(10, 2),
    sp1                 string,
    sp2                 string,
    sp3                 string,
    product_pic         string,
    product_name        string,
    product_sub_title   string,
    product_sku_code    string,
    member_nickname     string,
    create_date         timestamp,
    modify_date         timestamp,
    delete_status       int,
    product_category_id bigint,
    product_brand       string,
    product_sn          string,
    product_attr        string,
    start_dt            string,
    end_dt              string

)
    PARTITIONED BY (dt string)
    STORED AS orc
    TBLPROPERTIES (
        'orc.compress' = 'snappy'
        );

/*
    退货申请记录表  创建
 */
CREATE TABLE dwd.oms_order_return_apply_scd
(
    id                 bigint,
    order_id           bigint,
    company_address_id bigint,
    product_id         bigint,
    order_sn           string,
    create_time        timestamp,
    member_username    string,
    return_amount      decimal(10, 2),
    return_name        string,
    return_phone       string,
    status             int,
    handle_time        timestamp,
    product_pic        string,
    product_name       string,
    product_brand      string,
    product_attr       string,
    product_count      int,
    product_price      decimal(10, 2),
    product_real_price decimal(10, 2),
    reason             string,
    description        string,
    proof_pics         string,
    handle_note        string,
    handle_man         string,
    receive_man        string,
    receive_time       timestamp,
    receive_note       string,
    start_dt           string,
    end_dt             string
)
    PARTITIONED BY (dt string)
    STORED AS orc
    TBLPROPERTIES (
        'orc.compress' = 'snappy'
        );


/*
    标签结果表 创建
 */
drop table if exists dws.userprofile_order_cart_tag;
create table dws.userprofile_order_cart_tag
(
    guid                        bigint,--用户
    first_order_time            string,--首单日期
    last_order_time             string,--末单日期
    first_order_ago             bigint,--首单距今天数
    last_order_ago              bigint,--末单距今天数
    month1_order_cnt            bigint,--近30天下单次数
    month1_order_amt            double,--近30天订单金额（总金额）
    month2_order_cnt            bigint,--近60天下单次数
    month2_order_amt            double,--近60天订单金额
    month3_order_cnt            bigint,--近90天购买次数
    month3_order_amt            double,--近90天购买金额
    max_order_amt               double,--最大订单金额
    min_order_amt               double,--最小订单金额
    total_order_cnt             bigint,--累计订单数（不含退拒）
    total_order_amt             double,--累计消费金额（不含退拒）
    total_order_avg             double,--平均订单金额（不含退拒）
    month3_user_avg_amt         double, --近90天平均订单金额（不含退拒）
    total_coupon_amt            double,--累计使用代金券金额
    total_order_cnt_all         bigint, --累计订单数（含退拒）
    total_order_amt_all         double, --累计消费金额（含退拒）
    user_avg_order_amt_all      double, --平均订单金额（含退拒）
    month3_user_avg_amt_all     double, --近90天平均订单金额（含退拒）

    common_address              string, --最常用收货地址
    common_paytype              string, --最常用支付方式

    month1_cart_goods_cnt_30    bigint,--最近30天加购商品件数
    month1_cart_goods_amt_30    bigint,--最近30天加购商品金额
    month1_cart_goods_top_cat   bigint,--最近30天加购件数最多的商品品类id
    month1_cart_goods_top_brand bigint,--最近30天加购件数最多的品牌id
    month1_cart_cancel_cnt      bigint,--最近30天购物车取消商品件数
    month1_cart_cancel_amt      bigint --最近30天购物车取消商品金额
) partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy')
;

/*
    标签计算sql
 */
with od as (SELECT a.*,
                   if(b.order_id is null, 0, 1) as exist_return
            FROM (select *, to_date(create_time) as order_create_dt
                  from dwd.oms_order_scd
                  where dt = '20241110'
                    and (start_dt <= '2024-11-10' and end_dt >= '2024-11-10')) a
                     LEFT JOIN

                 (select order_id -- 表示这个订单是有退货存在
                  from dwd.oms_order_return_apply_scd
                  where dt = '20241110'
                    and (start_dt <= '2024-11-10' and end_dt >= '2024-11-10')
                  group by order_id) b)
   , cart as (select *
              from dwd.oms_cart_item_scd
              where dt = '20241110'
                and (start_dt <= '2024-11-10' and end_dt >= '2024-11-10'))

   , guids as (select member_id as guid
               from dwd.oms_order_scd
               where dt = '20241110'
                 and (start_dt <= '2024-11-10' and end_dt >= '2024-11-10')

               union
               -- 会对结果去重

               select member_id as guid
               from dwd.oms_cart_item_scd
               where dt = '20241110'
                 and (start_dt <= '2024-11-10' and end_dt >= '2024-11-10'))

insert
into table dws.userprofile_order_cart_tag partition (dt = '2024-11-10')
select guids.guid
     , part1.first_order_time         --首单日期
     , part1.last_order_time          --末单日期
     , part1.first_order_ago          --首单距今天数
     , part1.last_order_ago           --末单距今天数
     , part1.month1_order_cnt         --近30天下单次数
     , part1.month1_order_amt         --近30天订单金额（总金额）
     , part1.month2_order_cnt         --近60天下单次数
     , part1.month2_order_amt         --近60天订单金额
     , part1.month3_order_cnt         --近90天购买次数
     , part1.month3_order_amt         --近90天购买金额
     , part1.max_order_amt            --最大订单金额
     , part1.min_order_amt            --最小订单金额
     , part1.total_order_cnt          --累计订单数（不含退拒）
     , part1.total_order_amt          --累计消费金额（不含退拒）
     , part1.total_order_avg          --平均订单金额（不含退拒）
     , part1.month3_user_avg_amt      --近90天平均订单金额（不含退拒）
     , part1.total_coupon_amt         --累计使用代金券金额
     , part1.total_order_cnt_all      --累计订单数（含退拒）
     , part1.total_order_amt_all      --累计消费金额（含退拒）
     , part1.user_avg_order_amt_all   --平均订单金额（含退拒）
     , part1.month3_user_avg_amt_all  --近90天平均订单金额（含退拒）

     , part2.common_address           -- 最常用收货地址
     , part3.common_paytype           -- 最常用支付方式

     , part4.month1_cart_goods_cnt_30 --最近30天加购商品件数
     , part4.month1_cart_goods_amt_30 --最近30天加购商品金额
     , part4.month1_cart_cancel_cnt   --最近30天购物车取消商品件数
     , part4.month1_cart_cancel_amt   --最近30天购物车取消商品金额
     , part5.month1_cart_goods_top_cat
     , part6.month1_cart_goods_top_brand

from guids
         left join (
-- part1-tags
    select member_id                                                                  as guid
         , min(order_create_dt)                                                       as first_order_time        --首单日期
         , max(order_create_dt)                                                       as last_order_time         --末单日期
         , datediff('2024-11-10', min(order_create_dt))                               as first_order_ago         --首单距今天数
         , datediff('2024-11-10', max(order_create_dt))                               as last_order_ago          --末单距今天数
         , sum(if(datediff('2024-11-10', order_create_dt) <= 30, 1, 0))               as month1_order_cnt        --近30天下单次数
         , sum(if(datediff('2024-11-10', order_create_dt) <= 30, total_amount, 0))    as month1_order_amt        --近30天订单金额（总金额）
         , sum(if(datediff('2024-11-10', order_create_dt) <= 60, 1, 0))               as month2_order_cnt        --近60天下单次数
         , sum(if(datediff('2024-11-10', order_create_dt) <= 60, total_amount, 0))    as month2_order_amt        --近60天订单金额
         , sum(if(datediff('2024-11-10', order_create_dt) <= 90, 1, 0))               as month3_order_cnt        --近90天购买次数
         , sum(if(datediff('2024-11-10', order_create_dt) <= 90, total_amount, 0))    as month3_order_amt        --近90天购买金额
         , max(total_amount)                                                          as max_order_amt           --最大订单金额
         , min(total_amount)                                                          as min_order_amt           --最小订单金额
         , count(if(exist_return = 0, 1, null))                                       as total_order_cnt         --累计订单数（不含退拒）
         , sum(if(exist_return = 0, total_amount, 0))                                 as total_order_amt         --累计消费金额（不含退拒）
         , avg(if(exist_return = 0, total_amount, null))                              as total_order_avg         --平均订单金额（不含退拒）

         , avg(if(exist_return = 0 and datediff('2024-11-10', order_create_dt) <= 90, total_amount,
                  null))                                                              as month3_user_avg_amt     --近90天平均订单金额（不含退拒）
         , sum(coupon_amount)                                                         as total_coupon_amt        --累计使用代金券金额
         , count(1)                                                                   as total_order_cnt_all     --累计订单数（含退拒）
         , sum(total_amount)                                                          as total_order_amt_all     --累计消费金额（含退拒）
         , avg(total_amount)                                                          as user_avg_order_amt_all  --平均订单金额（含退拒）
         , avg(if(datediff('2024-11-10', order_create_dt) <= 90, total_amount, null)) as month3_user_avg_amt_all --近90天平均订单金额（含退拒）
    from od
    group by member_id) part1
                   on guids.guid = part1.guid

         left join (
-- part2-tags   : common_address                 string     , --最常用收货地址
-- 求每个用户的每个地址的使用次数
    select member_id as guid,
           address   as common_address -- 最常用收货地址
    from (select member_id,
                 concat_ws('|', receiver_province, receiver_city, receiver_region, receiver_post_code, receiver_phone,
                           receiver_detail_address)                                                     as address,
                 row_number() over (partition by member_id order by count(1)/* 地址的使用次数 */ desc ) as rn
          from od
          group by member_id,
                   concat_ws('|', receiver_province, receiver_city, receiver_region, receiver_post_code, receiver_phone,
                             receiver_detail_address)) o1
    where rn = 1) part2
                   on guids.guid = part2.guid

         left join (
-- part3-tags  : common_paytype                 string     , --最常用支付方式
    select member_id as guid,
           pay_type  as common_paytype -- 最常用支付方式
    from (select member_id,
                 pay_type,
                 row_number() over (partition by member_id order by count(1)/* 地址的使用次数 */ desc ) as rn
          from od
          group by member_id, pay_type) o2
    where rn = 1) part3
                   on guids.guid = part3.guid


         left join (
-- part4-tags
    select member_id                                                                                        as guid
         , sum(if(datediff('2024-11-10', to_date(create_date)), quantity, 0))                               as month1_cart_goods_cnt_30 --最近30天加购商品件数
         , sum(if(datediff('2024-11-10', to_date(create_date)), quantity * price,0))                        as month1_cart_goods_amt_30 --最近30天加购商品金额
         , sum(if(datediff('2024-11-10', to_date(modify_date)) and delete_status = 1, quantity,0))          as month1_cart_cancel_cnt   --最近30天购物车取消商品件数
         , sum(if(datediff('2024-11-10', to_date(modify_date)) and delete_status = 1, quantity * price,0))  as month1_cart_cancel_amt   --最近30天购物车取消商品金额
    from cart
    group by member_id) part4
                   on guids.guid = part4.guid


         left join (
-- part5 -tags : month1_cart_goods_top_cat   --最近30天加购件数最多的商品品类id

    SELECT member_id           as guid,
           product_category_id as month1_cart_goods_top_cat
    FROM (select member_id,
                 product_category_id,
                 row_number() over (partition by member_id order by sum(quantity) desc) as rn
          from cart
          where datediff('2024-11-10', to_date(create_date)) <= 30
          group by member_id, product_category_id) o3
    where rn = 1) part5
                   on guids.guid = part5.guid

         left join (
--part6-tags :  month1_cart_goods_top_brand --最近30天加购件数最多的品牌id
    SELECT member_id     as guid,
           product_brand as month1_cart_goods_top_brand
    FROM (select member_id,
                 product_brand,
                 row_number() over (partition by member_id order by sum(quantity) desc) as rn
          from cart
          where datediff('2024-11-10', to_date(create_date)) <= 30
          group by member_id, product_brand) o3
    where rn = 1) part6
                   on guids.guid = part6.guid
;

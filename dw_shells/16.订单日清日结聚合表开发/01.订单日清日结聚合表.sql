SELECT
    source_type
     ,order_type
     ,sum(if(to_date(create_time)= '2024-11-10',1,0)) as `新订单总数`
     ,sum(if(to_date(create_time)= '2024-11-10',total_amount,0)) as `新订单总额`
     ,sum(if(to_date(create_time)= '2024-11-10',pay_amount,0)) as `新订单应付总额`
     ,sum(if(to_date(create_time)= '2024-11-10' and status =0,1,0)) as `新订单待付总数`
     ,sum(if(to_date(create_time)= '2024-11-10' and status =0,total_amount,0)) as `新订单待付总额`

     ,sum(if(to_date(payment_time)= '2024-11-10' and status =0,1,0)) as `T日支付订单数`
     ,sum(if(to_date(payment_time)= '2024-11-10' and status =0,total_amount,0)) as `T日支付订单总额`

     ,sum(if(to_date(delivery_time)= '2024-11-10' and status =0,1,0)) as `T日发货订单数`
     ,sum(if(to_date(delivery_time)= '2024-11-10' and status =0,total_amount,0)) as `T日发货订单总额`


     ,sum(if(to_date(receive_time)= '2024-11-10' and status =0,1,0)) as `T日确认订单数`
     ,sum(if(to_date(receive_time)= '2024-11-10' and status =0,total_amount,0)) as `T日确认订单总额`


FROM dwd.oms_order_scd
WHERE dt='20241110'
  and dt between start_dt and end_dt
group by
    source_type  -- 订单来源
       ,order_type   -- 订单类型
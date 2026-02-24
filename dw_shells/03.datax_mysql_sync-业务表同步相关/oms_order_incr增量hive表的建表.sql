drop table if exists ods.oms_order_incr;
CREATE external TABLE ods.oms_order_incr (
  id bigint
  ,member_id bigint
  ,coupon_id bigint
  ,order_sn string
  ,create_time timestamp
  ,member_username string
  ,total_amount decimal(10,2)
  ,pay_amount decimal(10,2)
  ,freight_amount decimal(10,2)
  ,promotion_amount decimal(10,2)
  ,integration_amount decimal(10,2)
  ,coupon_amount decimal(10,2)
  ,discount_amount decimal(10,2)
  ,pay_type int
  ,source_type int
  ,status int
  ,order_type int
  ,delivery_company string
  ,delivery_sn string
  ,auto_confirm_day int
  ,integration int
  ,growth int
  ,promotion_info string
  ,bill_type int
  ,bill_header string
  ,bill_content string
  ,bill_receiver_phone string
  ,bill_receiver_email string
  ,receiver_name string
  ,receiver_phone string
  ,receiver_post_code string
  ,receiver_province string
  ,receiver_city string
  ,receiver_region string
  ,receiver_detail_address string
  ,note string
  ,confirm_status int
  ,delete_status int
  ,use_integration int
  ,payment_time timestamp
  ,delivery_time timestamp
  ,receive_time timestamp
  ,comment_time timestamp
  ,modify_time timestamp
)
partitioned by (dt string)
stored as orc
tblproperties(
 'orc.compress' = 'snappy'
);
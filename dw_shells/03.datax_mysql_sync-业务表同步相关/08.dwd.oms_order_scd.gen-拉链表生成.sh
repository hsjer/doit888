#!/bin/bash


##########################################
#  作者: 深似海的男人
#  日期：2024-10-29
#  功能：oms_order业务订单表的拉链表生成
#  联系：hitao919
##########################################


export HADOOP_HOME=/opt/app/hadoop-3.1.4/
export HIVE_HOME=/opt/app/hive-3.1.2/


TARGET_T=$(date -d'-1 day' +%Y%m%d)

if [ $1 ]
then
TARGET_T=$1

fi
T_PRE=$(date -d"${TARGET_T} -1 day" +%Y%m%d)


echo "------------------------------------------------------------------"
echo "脚本准备工作，oms_order拉链表生成的目标日期是: $TARGET_T                   "
echo "------------------------------------------------------------------"
sleep 2


sql="
WITH scd AS (
SELECT * FROM  dwd.oms_order_scd WHERE dt='${T_PRE}'  -- T-1
)
,inc AS (
SELECT * FROM ods.oms_order_incr WHERE dt='${TARGET_T}'  -- T
)

INSERT INTO TABLE dwd.oms_order_scd PARTITION(dt='${TARGET_T}')
SELECT
 scd.id
,scd.member_id
,scd.coupon_id
,scd.order_sn
,scd.create_time
,scd.member_username
,scd.total_amount
,scd.pay_amount
,scd.freight_amount
,scd.promotion_amount
,scd.integration_amount
,scd.coupon_amount
,scd.discount_amount
,scd.pay_type
,scd.source_type
,scd.status
,scd.order_type
,scd.delivery_company
,scd.delivery_sn
,scd.auto_confirm_day
,scd.integration
,scd.growth
,scd.promotion_info
,scd.bill_type
,scd.bill_header
,scd.bill_content
,scd.bill_receiver_phone
,scd.bill_receiver_email
,scd.receiver_name
,scd.receiver_phone
,scd.receiver_post_code
,scd.receiver_province
,scd.receiver_city
,scd.receiver_region
,scd.receiver_detail_address
,scd.note
,scd.confirm_status
,scd.delete_status
,scd.use_integration
,scd.payment_time
,scd.delivery_time
,scd.receive_time
,scd.comment_time
,scd.modify_time
,scd.start_dt
,if(scd.end_dt='99991231' AND inc.id IS NOT NULL , '${T_PRE}' ,scd.end_dt) as end_dt
FROM scd LEFT JOIN inc ON scd.id = inc.id

UNION ALL

SELECT
 id
,member_id
,coupon_id
,order_sn
,create_time
,member_username
,total_amount
,pay_amount
,freight_amount
,promotion_amount
,integration_amount
,coupon_amount
,discount_amount
,pay_type
,source_type
,status
,order_type
,delivery_company
,delivery_sn
,auto_confirm_day
,integration
,growth
,promotion_info
,bill_type
,bill_header
,bill_content
,bill_receiver_phone
,bill_receiver_email
,receiver_name
,receiver_phone
,receiver_post_code
,receiver_province
,receiver_city
,receiver_region
,receiver_detail_address
,note
,confirm_status
,delete_status
,use_integration
,payment_time
,delivery_time
,receive_time
,comment_time
,modify_time
,'${TARGET_T}' as start_dt
,'99991231' as end_dt
FROM inc
"



# 执行sql
${HIVE_HOME}/bin/hive  -e "${sql}"


# 判断拉链表生成作业是否执行成功
if [ $? -eq 0 ];then
# 发送成功的消息，给数据平台
echo "oms_order拉链表生成任务执行成功，增量表:ods.oms_order_incr,查询分区 :${TARGET_T} , 拉链表:dwd.oms_order_scd,查询分区:${T_PRE},目标分区:${TARGET_T}"
else

# 发送失败的消息，给数据平台
echo "oms_order拉链表生成任务执行失败，增量表:ods.oms_order_incr,查询分区 :${TARGET_T} , 拉链表:dwd.oms_order_scd,查询分区:${T_PRE},目标分区:${TARGET_T}"

fi
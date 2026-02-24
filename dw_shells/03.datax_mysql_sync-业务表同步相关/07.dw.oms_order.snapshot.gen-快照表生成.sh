#!/bin/bash


##########################################
#  作者: 深似海的男人
#  日期：2024-10-29
#  功能：统计日志采集的落地结果质量信息
#  联系：hitao919
##########################################


export HADOOP_HOME=/opt/app/hadoop-3.1.4/
export HIVE_HOME=/opt/app/hive-3.1.2/
export DATAX_HOME=/opt/app/datax

# 获取生成全量快照的目标分区
snapshot_partition_dt=$(date -d'-2 day' +%Y%m%d)
incr_partition_dt=$(date -d'-1 day' +%Y%m%d)


if [ $1 ]
then
# T日
incr_partition_dt=$1
# T-1日
snapshot_partition_dt=$(date -d "${1} -1 day" +%Y%m%d)
fi


sql="
with tmp as (
    select
    *
    from dwd.oms_order_snapshot
    where dt='${snapshot_partition_dt}'   -- T-1日全量

    UNION ALL

    select
    *
    from ods.oms_order_incr
    where dt='${incr_partition_dt}'   -- T日增量

)
insert into dwd.oms_order_snapshot partition(dt='${incr_partition_dt}')   -- 得到T的全量
select
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
from (
    select
    *,
    row_number() over(partition by id order by modify_time desc) as rn

    from tmp
) o
where rn=1
"


# 执行sql
${HIVE_HOME}/bin/hive -e "${sql}"


# 判断同步作业是否执行成功
if [ $? -eq 0 ];then
# 发送成功的消息，给数据平台
echo "oms_order全量快照生成任务执行成功，增量表:ods.oms_order_incr,查询分区 :${incr_partition_dt} , 全量表:dwd.oms_order_snapshot,查询分区:${snapshot_partition_dt},目标分区:${incr_partition_dt}"
else

# 发送失败的消息，给数据平台
echo "oms_order全量快照生成任务执行失败，增量表:ods.oms_order_incr,查询分区 :${incr_partition_dt} , 全量表:dwd.oms_order_snapshot,查询分区:${snapshot_partition_dt},目标分区:${incr_partition_dt}"
fi
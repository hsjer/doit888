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

# 获取一个同步数据的目标日期
target_dt=$(date -d'-1 day' +%Y-%m-%d)
partition_dt=$(date -d'-1 day' +%Y%m%d)

if [ $1 ]
then
target_dt=$1
partition_dt=$(date -d "${1}" +"%Y%m%d")


fi

echo "--------------------------------------------------------------------------"
echo "订单表同步作业准备启动，目标日期是: $target_dt  ,目标分区：$partition_dt   "
echo "--------------------------------------------------------------------------"
sleep 2


# 更新datax的配置文件，用实际目标日期去替换掉配置文件中的占位符
sed  "s/{TARGET_DT}/${target_dt}/g" ${DATAX_HOME}/datax.oms_order_incr.json > ${DATAX_HOME}/tmp.json
sed  "s/{PARTITION}/${partition_dt}/g" ${DATAX_HOME}/tmp.json > ${DATAX_HOME}/oms_order_incr_${target_dt}.json
rm -rf ${DATAX_HOME}/tmp.json


# 为增量表添加目标分区
${HIVE_HOME}/bin/hive -e "alter table ods.oms_order_incr add partition(dt='${partition_dt}')"


# 启动同步作业
python ${DATAX_HOME}/bin/datax.py ${DATAX_HOME}/oms_order_incr_${target_dt}.json


# 判断同步作业是否执行成功
if [ $? -eq 0 ];then
# 发送成功的消息，给数据平台
echo "同步作业执行成功，源表： oms_order , 目标表: ods.oms_order_incr ,增量日期： $target_dt "
else

# 发送失败的消息，给数据平台
echo "同步作业执行失败，源表： oms_order , 目标表: ods.oms_order_incr ,增量日期： $target_dt "
fi



# rm -rf ${DATAX_HOME}/oms_order_incr_${target_dt}.json

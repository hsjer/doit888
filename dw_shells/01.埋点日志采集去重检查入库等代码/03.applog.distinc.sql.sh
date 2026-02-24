#!/bin/bash


##########################################
#  作者: 深似海的男人
#  日期：2024-10-29
#  功能：统计日志采集的落地结果质量信息
#  联系：hitao919
##########################################


export HADOOP_HOME=/opt/app/hadoop-3.1.4/
export HIVE_HOME=/opt/app/hive-3.1.2/

target_dt=$(date -d'-1 day' +%Y-%m-%d)

if [ $1 ]
then
target_dt=$1
fi

echo "------------------------------------------------------------------"
echo "脚本准备工作，去重的目标日期是: $target_dt                        "
echo "------------------------------------------------------------------"
sleep 2

sql="
insert into ods.user_action_log partition(dt='${target_dt}')
select
    username
    ,app_id
    ,app_version
    ,release_channel
    ,carrier
    ,net_type
    ,ip
    ,device_id
    ,device_type
    ,resolution
    ,os_name
    ,os_version
    ,latitude
    ,longitude
    ,event_id
    ,properties
    ,action_time
    ,session_id
from (
select
    username
    ,app_id
    ,app_version
    ,release_channel
    ,carrier
    ,net_type
    ,ip
    ,device_id
    ,device_type
    ,resolution
    ,os_name
    ,os_version
    ,latitude
    ,longitude
    ,event_id
    ,properties
    ,action_time
    ,session_id
    ,row_number() over(
        partition by md5(concat_ws('',username
           ,app_id
           ,app_version
           ,release_channel
           ,carrier
           ,net_type
           ,ip
           ,device_id
           ,device_type
           ,resolution
           ,os_name
           ,os_version
           ,cast(latitude as string)
           ,cast(longitude as string)
           ,event_id
           ,cast(action_time as string)
           ,session_id))
    ) as rn

from  tmp.app_user_action_log
where dt='${target_dt}'
) o
where rn=1
"

# 执行sql
${HIVE_HOME}/bin/hive -e "${sql}"

if [ $? -ne 0 ]
then
# 请求公司的钉钉信息接口，发错误信息
echo "sql去重执行失败"
exit 1
else
echo "sql去重执行成功"
fi

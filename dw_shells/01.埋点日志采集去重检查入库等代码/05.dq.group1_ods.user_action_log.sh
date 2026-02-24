#!/bin/bash


##########################################
#  作者: 深似海的男人
#  日期：2024-10-29
#  功能：ods用户行为日志表的质量检查脚本
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
echo "脚本准备工作，质检的目标日期是: $target_dt                        "
echo "------------------------------------------------------------------"
sleep 2



# 未优化的写法
# select
#  count(if(username is null ,1,null)) as un_null  -- username空值个数
#  count(if(username is null ,1,null)) as ei_null  -- event_id 空值个数
#  max(action_time) as act_max   -- action_time最大值
#  min(action_time) as act_min   -- action_time最小值
# from user_action_log
# where dt='2024-10-28'


# 优化后的版本
sql="
select
    '_flag_' as flag
    ,sum(un_null) as un_null
    ,sum(ei_null) as ei_null
    ,max(act_max) as act_max
    ,min(act_min) as act_min
from (
    select
        count(if(username is null ,1,null)) as un_null
        ,count(if(event_id is null ,1,null)) as ei_null
        ,max(action_time) as act_max
        ,min(action_time) as act_min
    from ods.user_action_log
    where dt='${target_dt}'
    group by floor(rand()*1000)%20
) o
"

X=$(${HIVE_HOME}/bin/hive -S -e "${sql}" | grep '_flag_' | grep -v 'INFO' | grep -v 'WARN' )


un_null=$(echo $X | awk '{print $8}')
ei_null=$(echo $X | awk '{print $9}')
act_max=$(echo $X | awk '{print $10}')
act_min=$(echo $X | awk '{print $11}')


json="
{
    \"target_db\":\"ods\",
    \"target_table\":\"user_action_log\",
    \"rule_check_results\":[
    {
        \"rule_type\":\"0\",
        \"target_field\":\"username\",
        \"res_value\":${un_null}
    },
    {
        \"rule_type\":\"0\",
        \"target_field\":\"event_id\",
        \"res_value\":${ei_null}
    },
    {
        \"rule_type\":\"1\",
        \"target_field\":\"action_time\",
        \"res_value\":${act_max}
    },
    {
        \"rule_type\":\"2\",
        \"target_field\":\"action_time\",
        \"res_value\":${act_min}
    }

    ]
}
"

echo "即将上报统计结果到质量管理平台："
echo $json

# 发送检查结果到质量管理平台
# curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "${json}" "http://192.168.77.2:8080/api/dp/ods/applog"

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
echo "脚本准备工作，行数统计的目标日期是: $target_dt                        "
echo "------------------------------------------------------------------"
sleep 2

x=$(${HIVE_HOME}/bin/hive -S -e "select '^^^' as flag, sum(cnt) as cnt
                                 from (
                                 select count(1) as cnt
                                 from ods.user_action_log where dt='${target_dt}'
                                 group by  floor(rand()*100)%10
                                 ) o " | grep ^^^ | grep -v INFO)
line_amt=$(echo $x | awk '{print $2}')
echo "目标分区:${target_dt} , 表数据行数: ${line_amt} "


json="{
         \"database\":\"ods\",
         \"table\":\"user_action_log\",
         \"partition\":\"${target_dt}\",
         \"lines_amt\":${line_amt}
      }"
echo "即将上报统计结果到质量管理平台："
echo $json

# 发送检查结果到质量管理平台
curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "${json}" "http://192.168.77.2:8080/api/dp/ods/applog"

#!/bin/bash


##########################################
#  作者: 深似海的男人
#  日期：2024-10-29
#  功能：统计日志服务器上的app前端日志行数
#  联系：hitao919
##########################################


target_dt=$(date -d'-1 day' +%Y-%m-%d)
host=$(hostname)

if [ $1 ]
then
target_dt=$1
fi

echo "------------------------------------------------------------------"
echo "脚本准备工作，统计的目标日期是: $target_dt ,统计的机器是： $host  "
echo "------------------------------------------------------------------"
sleep 2


# 遍历统计每一个日志文件的行数
for f in /opt/data/app_log/log_${target_dt}*
do
line_cnt=$(wc -l $f | cut -d ' ' -f 1)
echo "file_name: ${f}"
echo "line_cnt: ${line_cnt}"


# 发送检查结果到质量管理平台
curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "
{
   \"log_server\":\"${host}\",
   \"file_name\":\"${f}\",
   \"line_cnt\": ${line_cnt},
   \"target_dt\":\"${target_dt}\"
}" "http://192.168.77.2:8080/api/dp/logserver/applog"


done
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
echo "脚本准备工作，统计的目标日期是: $target_dt                        "
echo "------------------------------------------------------------------"
sleep 2


# 统计落地端的文件个数
file_cnt=$(${HADOOP_HOME}/bin/hdfs dfs -count /events_log/app_log/normal/${target_dt} | awk '{print $2}')
echo "hdfs目标目录:${target_dt} ,文件个数统计结果: $file_cnt "

# 把目标落地目录，映射到临时表的分区
${HIVE_HOME}/bin/hive -e "alter table tmp.app_user_action_log add partition(dt='${target_dt}') location '/events_log/app_log/normal/${target_dt}'"
echo "目标目录:${target_dt} ,加载到临时表分区: ${target_dt} "

# 用sql统计行数
x=$(${HIVE_HOME}/bin/hive -S -e "select '^^^' as flag, sum(cnt) as cnt
                                 from (
                                 select count(1) as cnt
                                 from tmp.app_user_action_log where dt='${target_dt}'
                                 group by  floor(rand()*100)%10
                                 ) o " | grep ^^^ | grep -v INFO)
line_amt=$(echo $x | awk '{print $2}')
echo "目标目录:${target_dt} ,文件行数统计结果为: ${line_amt} "


json="{
         \"file_cnt\":${file_cnt},
         \"lines_amt\":${line_amt},
         \"target_dt\": \"${target_dt}\"
      }"
echo "即将上报统计结果到质量管理平台："
echo $json

# 发送检查结果到质量管理平台
curl -X POST -H "Content-Type: application/json" -H "Accept: application/json" -d "${json}" "http://192.168.77.2:8080/api/dp/hdfs/applog"

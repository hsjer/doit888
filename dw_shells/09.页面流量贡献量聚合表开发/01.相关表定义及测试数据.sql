/* **************************
    数据源测试表：行为日志明细表
 ******************************/
drop table if exists tmp.user_actionlog_detail_traffic;
create table tmp.user_actionlog_detail_traffic(
                                                  guid  bigint,
                                                  session_id string,
                                                  action_time bigint,
                                                  event_id    string,
                                                  page_url    string,
                                                  properties  map<string,string>
)
partitioned by (dt string)
row format delimited fields terminated by ','
collection items terminated by '_'
map keys terminated by ':';


/* 如下测试数据放入一个文件:  /root/traffic.txt 中
1,s01,1000,page_load,/a,xx:yy
1,s01,2000,page_load,/b,ref:/a
1,s01,3000,page_load,/c,ref:/a
1,s01,4000,page_load,/d,ref:/a
1,s01,5000,page_load,/e,ref:/c
1,s01,6000,page_load,/f,ref:/c
1,s01,7000,page_load,/a,ref:/f
1,s01,8000,page_load,/w,ref:/f
2,s02,21000,page_load,/a,xx:yy
2,s02,22000,page_load,/b,ref:/a
2,s02,23000,page_load,/c,ref:/a
2,s02,24000,page_load,/d,ref:/a
2,s02,25000,page_load,/e,ref:/c
2,s02,26000,page_load,/f,ref:/c
2,s02,27000,page_load,/a,ref:/f
2,s02,28000,page_load,/w,ref:/f
*/
load data local inpath '/root/traffic.txt' into table tmp.user_actionlog_detail_traffic partition(dt='20241101');


/* *****************************
    目标结果测试表：页面流量贡献量聚合表
 ******************************/
create table tmp.page_traffic_contribute(
                                            page_url string,
                                            direct_contribute bigint,
                                            whole_contribute  bigint
)
    partitioned by (dt string)
    stored as orc
    tblproperties(
        'orc.compress' = 'snappy'
        );
-- dwd层的行为日志明细数据建表
CREATE TABLE dwd.user_action_log_detail (
    -- 日志表中的字段
     username                      string
    ,app_id                        string
    ,app_version                   string
    ,release_channel               string
    ,carrier                       string
    ,net_type                      string
    ,ip                            string
    ,device_id                     string
    ,device_type                   string
    ,resolution                    string
    ,os_name                       string
    ,os_version                    string
    ,latitude                      double
    ,longitude                     double
    ,event_id                      string
    ,properties                    map<string,string>
    ,action_time                   bigint
    ,session_id                    string
    -- idmapping 增补的字段
    ,user_id                       bigint
    ,guid                          bigint

    -- 地理位置维表关联来的
    ,gps_province                string
    ,gps_city                    string
    ,gps_region                  string

    -- 用户注册信息表关联来的
    ,member_level_id             bigint
    ,nickname                    string
    ,phone                       string
    ,status                      int
    ,create_time                 timestamp
    ,icon                        string
    ,gender                      int
    ,birthday                    date
    ,city                        string
    ,job                         string
    ,personalized_signature      string
    ,source_type                 int
    ,integration                 int
    ,growth                      int
    ,luckey_count                int
    ,history_integration         int
    ,modify_time                 timestamp

    -- 页面信息维表关联来的
    ,url                         string
    ,page_type                   string
    ,business                    string
    ,channel                     string
    ,section                     string

)
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    TBLPROPERTIES(
        'orc.compress'='snappy'
        );



-- 注册  “ gps坐标转geohash码 ” 的函数
-- hive> add jar /root/udf.jar;
-- hive> create temporary function gps2geo as 'top.doe.hive.dataware.Gps2Geohash' ;
-- 此函数的代码，在 hive 项目中； top.doe.hive.dataware.Gps2Geohash

-- 用户注册信息表准备
with u as (
    select *
    from dwd.ums_member_scd
    where dt='20241101' and (dt between start_dt and end_dt)
)

-- 关联
INSERT INTO DWD.user_action_log_detail PARTITION (dt='20241101')
select
    a.username
     ,a.app_id
     ,a.app_version
     ,a.release_channel
     ,a.carrier
     ,a.net_type
     ,a.ip
     ,a.device_id
     ,a.device_type
     ,a.resolution
     ,a.os_name
     ,a.os_version
     ,a.latitude
     ,a.longitude
     ,a.event_id
     ,a.properties
     ,a.action_time
     ,a.session_id
     ,a.user_id
     ,a.guid

     ,b.province   as gps_province
     ,b.city       as gps_city
     ,b.region     as gps_region

     ,u.member_level_id
     ,u.nickname
     ,u.phone
     ,u.status
     ,u.create_time
     ,u.icon
     ,u.gender
     ,u.birthday
     ,u.city
     ,u.job
     ,u.personalized_signature
     ,u.source_type
     ,u.integration
     ,u.growth
     ,u.luckey_count
     ,u.history_integration
     ,u.modify_time

     ,a.properties['url'] as url
     ,p.page_type
     ,p.business
     ,p.channel
     ,p.section

from  tmp.log_guid a
left join dim.geohash_area b ON gps2geo(a.latitude,a.longitude) = b.geohash -- 关联地理位置
left join u ON a.user_id = u.id  -- 关联用户信息
left join dim.page_info p ON  regexp_extract(a.properties['url'],'(/.*/).*?',1) = p.url_prefix   -- /AABB/CC/dd/100101058195.html?SDGIASGJ  => /AABB/CC/dd/


-- 关键点:  full join  根据左右为空情况去使用不同的手段更新权重值


-- 创建 "设备-账号 绑定权重表"
CREATE DATABASE dws;
CREATE TABLE dws.device_username_bind(
                                         device_id    string,
                                         user_name    string,
                                         bind_weight  double,
                                         last_logintime bigint
)
    PARTITIONED BY (dt string)
STORED AS ORC
TBLPROPERTIES(
    'orc.compress' = 'snappy'
);


-- 滚动计算更新逻辑思考

-- T-1日，设备账号绑定表 (dws.device_username_bind)

/*
device_id,user_name,bind_weight,last_logintime
did_01,     aaa,         100,        tx
did_0c,     aaa,         80 ,        ty
did_0c,     bbb,         110,        tz
*/

insert into table dws.device_username_bind partition(dt='20241031') values
     ('did_01','aaa',100,1000)
    ,('did_0c','aaa',80,3000)
    ,('did_0c','bbb',110,2000);



-- T日，日志表 (ods.user_action_log)
--    账号, 会话id   , 设备id  , event_id  ,action_time
insert overwrite table ods.user_action_log partition(dt='20241101')  values
('aaa', 'top.doe.douyin','v3.2.0','app store',  '中国移动','5G','202.101.36.8','did_01','iphone 14','1920*1080','ios',     '10',39.88680452376819,116.36784267454223,'page_view',str_to_map('url:/mall/aaa/sdlj.html',',',':'),1730421427000,'session_01')
     ,('aaa', 'top.doe.douyin','v3.2.0','app store',  '中国移动','5G','202.101.36.8','did_01','iphone 14','1920*1080','ios',     '10',39.88680452376819,116.36784267454223,'page_view',str_to_map('url:/mall/aaa/sdlj.html',',',':'),1730421427000,'session_01')
     ,('aaa', 'top.doe.douyin','v3.2.0','app store',  '中国移动','5G','202.101.36.8','did_01','iphone 14','1920*1080','ios',     '10',39.88680452376819,116.36784267454223,'page_view',str_to_map('url:/mall/aaa/sdlj.html',',',':'),1730421427000,'session_01')
     ,('aaa', 'top.doe.douyin','v3.2.0','app store',  '中国移动','5G','202.101.36.8','did_01','iphone 14','1920*1080','ios',     '10',39.88680452376819,116.36784267454223,'page_view',str_to_map('url:/mall/aaa/sdlj.html',',',':'),1730421427000,'session_01')
     ,('aaa', 'top.doe.douyin','v3.2.0','app store',  '中国移动','5G','202.101.36.8','did_0c','iphone 14','1920*1080','ios',     '10',39.88680452376819,116.36784267454223,'page_view',str_to_map('url:/mall/aaa/sdlj.html',',',':'),1730421427000,'session_01')
     ,(null,  'top.doe.douyin','v3.2.0','小米应用',   '中国联通','5G','202.101.36.8','did_0c','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/aaa/sdlj.html',',',':'),1730421427000,'session_02')
     ,(null,  'top.doe.douyin','v3.2.0','小米应用',   '中国联通','5G','202.101.36.8','did_0c','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/aaa/sdlj.html',',',':'),1730421427000,'session_02')
     ,(null,  'top.doe.douyin','v3.2.0','小米应用',   '中国联通','5G','202.101.36.8','did_0c','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/aaa/sdlj.html',',',':'),1730421427000,'session_02')
     ,(null,  'top.doe.douyin','v3.2.0','小米应用',   '中国联通','5G','202.101.36.8','did_0c','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/aaa/sdlj.html',',',':'),1730421427000,'session_02')
     ,(null,  'top.doe.douyin','v3.2.0','小米应用',   '中国联通','5G','202.101.36.8','did_0c','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/aaa/sdlj.html',',',':'),1730421427000,'session_02')
     ,(null,  'top.doe.douyin','v3.2.0','小米应用',   '中国联通','5G','202.101.36.8','did_02','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/aaa/sdlj.html',',',':'),1730421427000,'session_02')
     ,(null,  'top.doe.douyin','v3.2.0','小米应用',   '中国联通','5G','202.101.36.8','did_02','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/aaa/sdlj.html',',',':'),1730421427000,'session_03')
     ,(null,  'top.doe.douyin','v3.2.0','小米应用',   '中国联通','5G','202.101.36.8','did_02','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/aaa/sdlj.html',',',':'),1730421427000,'session_03')
     ,(null,  'top.doe.douyin','v3.2.0','小米应用',   '中国联通','5G','202.101.36.8','did_02','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/bbb/2533.html',',',':'),1730421427000,'session_03')
     ,(null,  'top.doe.douyin','v3.2.0','小米应用',   '中国联通','5G','202.101.36.8','did_02','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/bbb/2533.html',',',':'),1730421427000,'session_03')
     ,(null,  'top.doe.douyin','v3.2.0','小米应用',   '中国联通','5G','202.101.36.8','did_02','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/bbb/2533.html',',',':'),1730421427000,'session_03')
     ,(null,  'top.doe.douyin','v3.2.0','小米应用',   '中国联通','5G','202.101.36.8','did_02','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/bbb/2533.html',',',':'),1730421427000,'session_03')
     ,('ccc', 'top.doe.douyin','v3.2.2','小米应用',   '中国移动','5G','202.101.36.8','did_02','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/bbb/2533.html',',',':'),1730421427000,'session_04')
     ,('ccc', 'top.doe.douyin','v3.2.2','小米应用',   '中国移动','5G','202.101.36.8','did_02','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/bbb/2533.html',',',':'),1730421427000,'session_04')
     ,('ccc', 'top.doe.douyin','v3.2.2','小米应用',   '中国移动','5G','202.101.36.8','did_02','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/bbb/2533.html',',',':'),1730421427000,'session_04')
     ,('ccc', 'top.doe.douyin','v3.2.2','小米应用',   '中国移动','5G','202.101.36.8','did_02','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/bbb/2533.html',',',':'),1730421427000,'session_04')
     ,('ccc', 'top.doe.douyin','v3.2.2','小米应用',   '中国移动','5G','202.101.36.8','did_02','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/bbb/2533.html',',',':'),1730421427000,'session_04')
     ,('ccc', 'top.doe.douyin','v3.2.2','小米应用',   '中国移动','5G','202.101.36.8','did_01','xiaomi 14','1920*1080','android', '9', 38.06422976145662,114.45971527739797,'page_view',str_to_map('url:/mall/bbb/2533.html',',',':'),1730421427000,'session_04')
     ,(null,  'top.doe.douyin','v3.2.3','apple store','中国移动','5G','202.101.36.8','did_01','iphone 14','1920*1080','ios',     '10',39.88680452376819,116.36784267454223,'page_view',str_to_map('url:/mall/bbb/2533.html',',',':'),1730421427000,'session_05')
     ,(null,  'top.doe.douyin','v3.2.3','apple store','中国移动','5G','202.101.36.8','did_01','iphone 14','1920*1080','ios',     '10',39.88680452376819,116.36784267454223,'page_view',str_to_map('url:/mall/bbb/2533.html',',',':'),1730421427000,'session_05')
     ,(null,  'top.doe.douyin','v3.2.3','apple store','中国移动','5G','202.101.36.8','did_01','iphone 14','1920*1080','ios',     '10',39.88680452376819,116.36784267454223,'page_view',str_to_map('url:/mall/bbb/2533.html',',',':'),1730421427000,'session_05')
     ,(null,  'top.doe.douyin','v3.2.3','apple store','中国移动','5G','202.101.36.8','did_01','iphone 14','1920*1080','ios',     '10',39.88680452376819,116.36784267454223,'page_view',str_to_map('url:/mall/bbb/2533.html',',',':'),1730421427000,'session_05')
;


-- 生成T日设备绑定表 (
--  1. "设备-账号"组合如果原来有，T日又出现，则加分；否则减分；
--  2. 新出现的组合追加进表）
/*
did_01, aaa, 100+10  ,t004
did_0c, aaa, 80*0.4  ,ty
did_0c, bbb, 110*0.4 ,tz
did_0c, ccc, 10      ,t033
*/


-- 正式sql


-- 1.T日的日志中的  设备-账号  组合，及其 会话数，最大时间
WITH new_bind AS (
    SELECT device_id,
    username as user_name,
    count(distinct session_id) as ses_cnt,
    max(action_time) as last_logintime
    FROM ods.user_action_log
    WHERE dt='20241101' and username is not null
    GROUP BY username,device_id
    ),
    old_bind as (
    SELECT * FROM dws.device_username_bind WHERE dt='20241031'
    )

-- 2.拿T-1绑定表 FULL JOIN  T日的设备-账号
INSERT INTO TABLE dws.device_username_bind PARTITION(dt='20241101')
SELECT
    nvl(old_bind.device_id,new_bind.device_id) as device_id,
    nvl(old_bind.user_name,new_bind.user_name) as user_name,
    -- 如果老组合今天又出现，则加分，否则衰减
    case
        when  old_bind.device_id is not null and new_bind.device_id is not null then bind_weight+ses_cnt*10
        when  old_bind.device_id is not null and new_bind.device_id is     null then bind_weight*0.4
        else  ses_cnt*10
        end as bind_weight,

    nvl(new_bind.last_logintime,old_bind.last_logintime) as last_logintime
FROM  old_bind  FULL JOIN new_bind
                          ON old_bind.device_id = new_bind.device_id
                              AND old_bind.user_name = new_bind.user_name
;
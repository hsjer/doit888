
alter table dwd.user_action_log_detail drop partition(dt='20241112');
insert into table dwd.user_action_log_detail partition(dt='20241112') (guid,event_id,action_time,properties)  values
(1,'page_load', 1731387600000,str_to_map('url:/a/b|title:小米空调挂机1.5/2匹 巨省电 新一级能效 节能变频冷暖 智能自清洁 壁挂式卧室空调挂机 以旧换新 大1匹 一级能效 巨省电26GW/S1A1','\\|',':'))
,(1,'page_load', 1731387610000,str_to_map('url:/a/b|title:小米（MI）米家空调挂机 新能效 变频冷暖智能自清洁壁挂式节能省电家用卧室舒适空调 1.5匹 一级能效 （巨省电35N1A1）','\\|',':'))
,(1,'page_load', 1731387620000,str_to_map('url:/a/b|title:【小米Redmi 12C】小米（MI）Redmi 12C Helio G85 性能芯 5000万高清双摄 5000mAh长续航 4GB+128GB 深海蓝 智能手机小米红米【行情 报价 价格 评测】-京东','\\|',':'))

;

/*
 +-------+------------+----------------+----------------------------------------------------+
| guid  |  event_id  |  action_time   |                     properties                     |
+-------+------------+----------------+----------------------------------------------------+
| 1     | page_load  | 1731387600000  | {"url":"/a/b","title":"小米空调挂机1.5/2匹 巨省电 新一级能效 节能变频冷暖 智能自清洁 壁挂式卧室空调挂机 以旧换新 大1匹 一级能效 巨省电26GW/S1A1"} |
| 1     | page_load  | 1731387610000  | {"url":"/a/b","title":"小米（MI）米家空调挂机 新能效 变频冷暖智能自清洁壁挂式节能省电家用卧室舒适空调 1.5匹 一级能效 （巨省电35N1A1）"} |
| 1     | page_load  | 1731387620000  | {"url":"/a/b","title":"【小米Redmi 12C】小米（MI）Redmi 12C Helio G85 性能芯 5000万高清双摄 5000mAh长续航 4GB+128GB 深海蓝 智能手机小米红米【行情 报价 价格 评测】-京东"} |
+-------+------------+----------------+----------------------------------------------------+
*/




-- 标签表创建
create table dws.profile_interest_words(
    guid bigint,
    view_words string,
    search_words string,
    favor_words string,
    dianzan_words string
)
partitioned by (dt string)
stored as orc
tblproperties (
    'orc.compress' = 'snappy'
) ;
-- 建表
-- /mall/aaa/,商品详情页,商城业务,频道A,栏目X
create table dim.page_info(
    url_prefix string,
    page_type  string,
    business   string,  -- 所属业务
    channel    string,  -- 频道
    section    string   -- 栏目
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'snappy'
    );



-- 导入数据过程
-- 1. 先创建一个文本格式的临时表
create table tmp.page_info(
                              url_prefix string,
                              page_type  string,
                              business   string,  -- 所属业务
                              channel    string,  -- 频道
                              section    string   -- 栏目
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE ;

-- 2.加载前端小姐姐发给我们的页面信息维度数据文本文件
LOAD DATA LOCAL INPATH '/root/page.txt' INTO TABLE tmp.page_info;

-- 3.从临时表转到维表
INSERT INTO TABLE dim.page_info
SELECT * FROM tmp.page_info;
drop table if exists tmp.page_info;
-- 建表
create database dim;
create table dim.device_ano_tmpid(
    device_id   string,
    tmpid       bigint,
    create_time bigint
)
stored as orc
tblproperties (
    'orc.compress'='snappy'
);


-- 测试数据
insert into table dim.device_ano_tmpid
values
('dx01',100000001,1730223000000),
('dx02',100000002,1730223020000),
('dx03',100000003,1730223100000);
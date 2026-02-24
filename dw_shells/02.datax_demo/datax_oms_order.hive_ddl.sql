drop table if exists ods.order_tj;
create table ods.order_tj(pid int,order_count int,user_count int,amt string)
    stored as orc
tblproperties('orc.compress'='snappy');
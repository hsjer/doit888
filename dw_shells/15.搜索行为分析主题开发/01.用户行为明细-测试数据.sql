alter table dwd.user_action_log_detail drop partition(dt='20241110');
insert into table dwd.user_action_log_detail partition(dt='20241110')
(create_time,member_level_id,gender,city,job,guid,url,event_id,action_time,properties)
values

    ('2024-11-10 08:32:28',1,1,'九江','公务员',1,'/page/y/',"event_x",10000,str_to_map('','_',':')                ),
    ('2024-11-10 08:32:28',1,1,'九江','公务员',1,'/page/y/',"search",20000,str_to_map('keyword:咖啡_sid:sh001_abtest:a','_',':')                ),
    ('2024-11-10 08:32:28',1,1,'九江','公务员',1,'/page/y/',"search_return",30000,str_to_map('keyword:咖啡_sid:sh001_abtest:a_returned:120','_',':')                ),
    ('2024-11-10 08:32:28',1,1,'九江','公务员',1,'/page/y/',"search_click", 40000,str_to_map('keyword:咖啡_sid:sh001_abtest:a_itemid:item001_itemseq:5','_',':')    ),
    ('2024-11-10 08:32:28',1,1,'九江','公务员',1,'/page/y/',"search_click",50000,str_to_map('keyword:咖啡_sid:sh001_abtest:a_itemid:item003_itemseq:2','_',':')     ),
    ('2024-11-10 08:32:28',1,1,'九江','公务员',1,'/page/y/',"search_click",80000,str_to_map('keyword:咖啡_sid:sh001_abtest:a_itemid:item008_itemseq:3','_',':')     ),

    ('2024-11-10 08:32:28',2,0,'镇江','程序员',2,'/page/y/',"event_x",10000,str_to_map('','_',':')                ),
    ('2024-11-10 08:32:28',2,0,'镇江','程序员',2,'/page/y/',"search",20000,str_to_map('keyword:奶茶_sid:sh002_abtest:b','_',':')                ),
    ('2024-11-10 08:32:28',2,0,'镇江','程序员',2,'/page/y/',"search_return",30000,str_to_map('keyword:奶茶_sid:sh002_abtest:b_returned:220','_',':')                ),
    ('2024-11-10 08:32:28',2,0,'镇江','程序员',2,'/page/y/',"search_click", 40000,str_to_map('keyword:奶茶_sid:sh002_abtest:b_itemid:item101_itemseq:1','_',':')    ),
    ('2024-11-10 08:32:28',2,0,'镇江','程序员',2,'/page/y/',"search_click",50000,str_to_map('keyword:奶茶_sid:sh002_abtest:b_itemid:item103_itemseq:4','_',':')     ),
    ('2024-11-10 08:32:28',2,0,'镇江','程序员',2,'/page/y/',"search_click",80000,str_to_map('keyword:奶茶_sid:sh002_abtest:b_itemid:item108_itemseq:6','_',':')     ),

    ('2024-11-10 08:32:28',2,1,'九江','程序员',3,'/page/y/',"event_x",10000,str_to_map('','_',':')                ),
    ('2024-11-10 08:32:28',2,1,'九江','程序员',3,'/page/y/',"search",20000,str_to_map('keyword:奶茶_sid:sh002_abtest:a','_',':')                ),
    ('2024-11-10 08:32:28',2,1,'九江','程序员',3,'/page/y/',"search_return",30000,str_to_map('keyword:奶茶_sid:sh003_abtest:a_returned:220','_',':')                ),
    ('2024-11-10 08:32:28',2,1,'九江','程序员',3,'/page/y/',"search_click", 40000,str_to_map('keyword:奶茶_sid:sh003_abtest:a_itemid:item101_itemseq:5','_',':')    ),
    ('2024-11-10 08:32:28',2,1,'九江','程序员',3,'/page/y/',"search_click",50000,str_to_map('keyword:奶茶_sid:sh003_abtest:a_itemid:item103_itemseq:10','_',':')     ),
    ('2024-11-10 08:32:28',2,1,'九江','程序员',3,'/page/y/',"search_click",80000,str_to_map('keyword:奶茶_sid:sh003_abtest:a_itemid:item112_itemseq:15','_',':')     ),
    ('2024-11-10 08:32:28',2,1,'九江','程序员',3,'/page/y/',"search_click",80000,str_to_map('keyword:奶茶_sid:sh003_abtest:a_itemid:item132_itemseq:3','_',':')     ),
    ('2024-11-10 08:32:28',2,1,'九江','程序员',3,'/page/y/',"search_click",80000,str_to_map('keyword:奶茶_sid:sh003_abtest:a_itemid:item166_itemseq:2','_',':')     ),

    ('2024-11-10 08:32:28',1,0,'镇江','设计师',4,'/page/y/',"event_x",10000,str_to_map('','_',':')                ),
    ('2024-11-10 08:32:28',1,0,'镇江','设计师',4,'/page/y/',"search",20000,str_to_map('keyword:咖啡_sid:sh001_abtest:a','_',':')                ),
    ('2024-11-10 08:32:28',1,0,'镇江','设计师',4,'/page/y/',"search_return",30000,str_to_map('keyword:咖啡_sid:sh004_abtest:a_returned:120','_',':')                ),
    ('2024-11-10 08:32:28',1,0,'镇江','设计师',4,'/page/y/',"search_click", 40000,str_to_map('keyword:咖啡_sid:sh004_abtest:a_itemid:item002_itemseq:4','_',':')    ),
    ('2024-11-10 08:32:28',1,0,'镇江','设计师',4,'/page/y/',"search_click",50000,str_to_map('keyword:咖啡_sid:sh004_abtest:a_itemid:item006_itemseq:1','_',':')     ),
    ('2024-11-10 08:32:28',1,0,'镇江','设计师',4,'/page/y/',"search_click",80000,str_to_map('keyword:咖啡_sid:sh004_abtest:a_itemid:item003_itemseq:2','_',':')     ),


    ('2024-11-10 08:32:28',1,0,'无锡','设计师',5,'/page/y/',"event_x",10000,str_to_map('','_',':')                ),
    ('2024-11-10 08:32:28',1,0,'无锡','设计师',5,'/page/y/',"search",20000,str_to_map('keyword:盐焗鸡_sid:sh001_abtest:a','_',':')                ),
    ('2024-11-10 08:32:28',1,0,'无锡','设计师',5,'/page/y/',"search_return",30000,str_to_map('keyword:盐焗鸡_sid:sh001_abtest:a_returned:3','_',':')                )

;

-- 检查数据
select  create_time,member_level_id,gender,city,job,guid,url,event_id,action_time,properties from dwd.user_action_log_detail where dt='20241110';
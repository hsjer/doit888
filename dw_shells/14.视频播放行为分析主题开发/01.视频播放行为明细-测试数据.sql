

alter table dwd.user_action_log_detail drop partition(dt='20241110');
insert into table dwd.user_action_log_detail partition(dt='20241110')
(create_time,member_level_id,gender,city,job,guid,url,event_id,action_time,properties)
values

    ('2024-11-10 08:32:28',1,1,'九江','化妆师',1,'/page/y/',"event_x",10000,str_to_map('','_',':')                ),
    ('2024-11-10 08:32:28',1,1,'九江','化妆师',1,'/page/y/',"video_play",20000,str_to_map('videoid:1_playid:pl001','_',':')                ),
    ('2024-11-10 08:32:28',1,1,'九江','化妆师',1,'/page/y/',"video_hb",30000,str_to_map('videoid:1_playid:pl001','_',':')                ),
    ('2024-11-10 08:32:28',1,1,'九江','化妆师',1,'/page/y/',"video_hb", 40000,str_to_map('videoid:1_playid:pl001','_',':')    ),
    ('2024-11-10 08:32:28',1,1,'九江','化妆师',1,'/page/y/',"video_pause",50000,str_to_map('videoid:1_playid:pl001','_',':')                  ),
    ('2024-11-10 08:32:28',1,1,'九江','化妆师',1,'/page/y/',"video_resume",80000,str_to_map('videoid:1_playid:pl001','_',':')                  ),
    ('2024-11-10 08:32:28',1,1,'九江','化妆师',1,'/page/y/',"video_hb",85000,str_to_map('videoid:1_playid:pl001','_',':')         ),
    ('2024-11-10 08:32:28',1,1,'九江','化妆师',1,'/page/y/',"video_hb",90000,str_to_map('videoid:1_playid:pl001','_',':')                  ),
    ('2024-11-10 08:32:28',1,1,'九江','化妆师',1,'/page/y/',"video_stop",95000,str_to_map('videoid:1_playid:pl001','_',':')                  ),
    ('2024-11-10 08:32:28',2,0,'无锡','销售员',2,'/page/w/',"video_play",10000,str_to_map('videoid:2_playid:pl002','_',':')                   ),
    ('2024-11-10 08:32:28',2,0,'无锡','销售员',2,'/page/w/',"video_hb",15000,str_to_map('videoid:2_playid:pl002','_',':')                   ),
    ('2024-11-10 08:32:28',2,0,'无锡','销售员',2,'/page/w/',"video_hb",20000,str_to_map('videoid:2_playid:pl002','_',':')             ),
    ('2024-11-10 08:32:28',2,0,'无锡','销售员',2,'/page/w/',"video_hb",25000,str_to_map('videoid:2_playid:pl002','_',':')               ),
    ('2024-11-10 08:32:28',2,0,'无锡','销售员',2,'/page/w//',"video_hb",30000,str_to_map('videoid:2_playid:pl002','_',':')     ),
    ('2024-11-10 08:32:28',2,0,'无锡','销售员',2,'/page/w/',"video_stop",33000,str_to_map('videoid:2_playid:pl002','_',':')     ),
    ('2024-11-10 08:32:28',1,0,'九江','化妆师',1,'/page/y/',"event_z",200000,str_to_map('','_',':')                ),
    ('2024-11-10 08:32:28',1,0,'九江','化妆师',1,'/page/y/',"video_play",210000,str_to_map('videoid:1_playid:pl003','_',':')                ),
    ('2024-11-10 08:32:28',1,0,'九江','化妆师',1,'/page/y/',"video_hb",215000,str_to_map('videoid:1_playid:pl003','_',':')                ),
    ('2024-11-10 08:32:28',1,0,'九江','化妆师',1,'/page/y/',"video_hb", 220000,str_to_map('videoid:1_playid:pl003','_',':')    ),
    ('2024-11-10 08:32:28',1,0,'九江','化妆师',1,'/page/y/',"video_pause",222000,str_to_map('videoid:1_playid:pl003','_',':')                  ),
    ('2024-11-10 08:32:28',1,0,'九江','化妆师',1,'/page/y/',"video_resume",315000,str_to_map('videoid:1_playid:pl003','_',':')                  ),
    ('2024-11-10 08:32:28',1,0,'九江','化妆师',1,'/page/y/',"video_hb",320000,str_to_map('videoid:1_playid:pl003','_',':')         ),
    ('2024-11-10 08:32:28',1,0,'九江','化妆师',1,'/page/y/',"video_hb",325000,str_to_map('videoid:1_playid:pl003','_',':')                  ),
    ('2024-11-10 08:32:28',1,0,'九江','化妆师',1,'/page/y/',"video_stop",330000,str_to_map('videoid:1_playid:pl003','_',':')                  ),
    ('2024-11-10 08:32:28',2,1,'九江','化妆师',3,'/page/y/',"event_w",300000,str_to_map('','_',':')                ),
    ('2024-11-10 08:32:28',2,1,'九江','化妆师',3,'/page/y/',"video_play",310000,str_to_map('videoid:2_playid:pl004','_',':')                ),
    ('2024-11-10 08:32:28',2,1,'九江','化妆师',3,'/page/y/',"video_hb",315000,str_to_map('videoid:2_playid:pl004','_',':')                ),
    ('2024-11-10 08:32:28',2,1,'九江','化妆师',3,'/page/y/',"video_hb", 320000,str_to_map('videoid:2_playid:pl004','_',':')    ),
    ('2024-11-10 08:32:28',2,1,'九江','化妆师',3,'/page/y/',"video_pause",322000,str_to_map('videoid:2_playid:pl004','_',':')                  ),
    ('2024-11-10 08:32:28',2,1,'九江','化妆师',3,'/page/y/',"video_resume",415000,str_to_map('videoid:2_playid:pl004','_',':')                  ),
    ('2024-11-10 08:32:28',2,1,'九江','化妆师',3,'/page/y/',"video_hb",420000,str_to_map('videoid:2_playid:pl004','_',':')         ),
    ('2024-11-10 08:32:28',2,1,'九江','化妆师',3,'/page/y/',"video_hb",425000,str_to_map('videoid:2_playid:pl004','_',':')                  ),
    ('2024-11-10 08:32:28',2,1,'九江','化妆师',3,'/page/y/',"video_stop",440000,str_to_map('videoid:2_playid:pl004','_',':')                  )
  ;

-- 检查数据

select  create_time,member_level_id,gender,city,job,guid,url,event_id,action_time,properties from dwd.user_action_log_detail where dt='20241110';

/*
+------------------------+------------------+---------+-------+------+-------+------------+---------------+--------------+-----------------------------------+
|      create_time       | member_level_id  | gender  | city  | job  | guid  |    url     |   event_id    | action_time  |            properties             |
+------------------------+------------------+---------+-------+------+-------+------------+---------------+--------------+-----------------------------------+
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | event_x       | 10000        | {"":null}                         |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | video_play    | 20000        | {"videoid":"1","playid":"pl001"}  |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | video_hb      | 30000        | {"videoid":"1","playid":"pl001"}  |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | video_hb      | 40000        | {"videoid":"1","playid":"pl001"}  |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | video_pause   | 50000        | {"videoid":"1","playid":"pl001"}  |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | video_resume  | 80000        | {"videoid":"1","playid":"pl001"}  |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | video_hb      | 85000        | {"videoid":"1","playid":"pl001"}  |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | video_hb      | 90000        | {"videoid":"1","playid":"pl001"}  |
| 2024-11-10 08:32:28.0  | 1                | 1       | 九江    | 化妆师  | 1     | /page/y/   | video_stop    | 95000        | {"videoid":"1","playid":"pl001"}  |

| 2024-11-10 08:32:28.0  | 2                | 0       | 无锡    | 销售员  | 2     | /page/w/   | video_play    | 10000        | {"videoid":"2","playid":"pl002"}  |
| 2024-11-10 08:32:28.0  | 2                | 0       | 无锡    | 销售员  | 2     | /page/w/   | video_hb      | 15000        | {"videoid":"2","playid":"pl002"}  |
| 2024-11-10 08:32:28.0  | 2                | 0       | 无锡    | 销售员  | 2     | /page/w/   | video_hb      | 20000        | {"videoid":"2","playid":"pl002"}  |
| 2024-11-10 08:32:28.0  | 2                | 0       | 无锡    | 销售员  | 2     | /page/w/   | video_hb      | 25000        | {"videoid":"2","playid":"pl002"}  |
| 2024-11-10 08:32:28.0  | 2                | 0       | 无锡    | 销售员  | 2     | /page/w//  | video_hb      | 30000        | {"videoid":"2","playid":"pl002"}  |
| 2024-11-10 08:32:28.0  | 2                | 0       | 无锡    | 销售员  | 2     | /page/w/   | video_stop    | 33000        | {"videoid":"2","playid":"pl002"}  |

| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | event_z       | 200000       | {"":null}                         |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | video_play    | 210000       | {"videoid":"1","playid":"pl003"}  |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | video_hb      | 215000       | {"videoid":"1","playid":"pl003"}  |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | video_hb      | 220000       | {"videoid":"1","playid":"pl003"}  |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | video_pause   | 222000       | {"videoid":"1","playid":"pl003"}  |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | video_resume  | 315000       | {"videoid":"1","playid":"pl003"}  |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | video_hb      | 320000       | {"videoid":"1","playid":"pl003"}  |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | video_hb      | 325000       | {"videoid":"1","playid":"pl003"}  |
| 2024-11-10 08:32:28.0  | 1                | 0       | 九江    | 化妆师  | 1     | /page/y/   | video_stop    | 330000       | {"videoid":"1","playid":"pl003"}  |

| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | event_w       | 300000       | {"":null}                         |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | video_play    | 310000       | {"videoid":"2","playid":"pl004"}  |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | video_hb      | 315000       | {"videoid":"2","playid":"pl004"}  |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | video_hb      | 320000       | {"videoid":"2","playid":"pl004"}  |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | video_pause   | 322000       | {"videoid":"2","playid":"pl004"}  |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | video_resume  | 415000       | {"videoid":"2","playid":"pl004"}  |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | video_hb      | 420000       | {"videoid":"2","playid":"pl004"}  |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | video_hb      | 425000       | {"videoid":"2","playid":"pl004"}  |
| 2024-11-10 08:32:28.0  | 2                | 1       | 九江    | 化妆师  | 3     | /page/y/   | video_stop    | 440000       | {"videoid":"2","playid":"pl004"}  |
+------------------------+------------------+---------+-------+------+-------+------------+---------------+--------------+-----------------------------------+


*/

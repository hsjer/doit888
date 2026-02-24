create table dws.actionlog_videoplay_agg_vd
(
    register_time         timestamp,  -- 用户注册时间
    member_level_id       int,        -- 会员等级id
    gender                string,     -- 性别
    city                  string,     -- 用户注册城市
    job                   string,     -- 用户职业
    guid                  bigint,     -- 用户全局标识
    url                   string,     -- 页面
    video_id              bigint,     -- 视频id
    video_name            string,     -- 视频名称
    video_type            string,     -- 视频类型
    video_album           string,     -- 视频专辑
    video_author          string,     -- 视频作者
    video_timelong        bigint,     -- 视频时长
    create_dt             string,     -- 视频发布日期

    play_count            bigint,     -- 播放次数
    play_users            string,     -- 播放人数
    play_amt_time         bigint,     -- 视频播放总时长
    play_complete_count   bigint,     -- 视频完播次数
    play_complete_users   bigint,     -- 完播人数
    play_jump_count       bigint,     -- 视频跳出次数
    play_jump_users       bigint      -- 完播跳出人数
)
PARTITIONED BY (dt string)
STORED AS orc
TBLPROPERTIES (
    'orc.compress'='snappy'
    )
;
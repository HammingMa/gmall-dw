use gmall;

drop table if exists dwd_dim_user_info_his;
create external table dwd_dim_user_info_his(
    `id`            string COMMENT '用户 id',
    `name`          string COMMENT '姓名',
    `birthday`      string COMMENT '生日',
    `gender`        string COMMENT '性别',
    `email`         string COMMENT '邮箱',
    `user_level`    string COMMENT '用户等级',
    `create_time`   string COMMENT '创建时间',
    `operate_time`  string COMMENT '操作时间',
    `start_date`    string COMMENT '有效开始日期',
    `end_date`      string COMMENT '有效结束日期'
) COMMENT '订单拉链表'
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_user_info_his/'
    tblproperties ("parquet.compression"="lzo");


drop table if exists tmp_dwd_dim_user_info_01;
create table tmp_dwd_dim_user_info_01 as
select
    id            , -- 用户 id
    name          , -- 姓名
    birthday      , -- 生日
    gender        , -- 性别
    email         , -- 邮箱
    user_level    , -- 用户等级
    create_time   , -- 创建时间
    operate_time  , -- 操作时间
    start_date    , -- 有效开始日期
    end_date        -- 有效结束日期
from
    (
        select
            id            , -- 用户 id
            name          , -- 姓名
            birthday      , -- 生日
            gender        , -- 性别
            email         , -- 邮箱
            user_level    , -- 用户等级
            create_time   , -- 创建时间
            operate_time  , -- 操作时间
            '$do_date' as start_date    , -- 有效开始日期
            '9999-99-99' as end_date        -- 有效结束日期
        from ods_user_info
        where dt ='$do_date'
        union all
        select
            a.id            , -- 用户 id
            a.name          , -- 姓名
            a.birthday      , -- 生日
            a.gender        , -- 性别
            a.email         , -- 邮箱
            a.user_level    , -- 用户等级
            a.create_time   , -- 创建时间
            a.operate_time  , -- 操作时间
            a.start_date    , -- 有效开始日期
            if(b.id is not null and a.end_date='9999-99-99', date_add('$do_date',-1),a.end_date) as end_date        -- 有效结束日期
        from dwd_dim_user_info_his a
                 left join
             (select * from ods_user_info where dt ='$do_date') b
             on a.id =b.id
    ) a;

insert overwrite table dwd_dim_user_info_his
select * from tmp_dwd_dim_user_info_01;





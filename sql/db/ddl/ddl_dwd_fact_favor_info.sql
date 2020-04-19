use gmall;


drop table if exists dwd_fact_favor_info; create external table dwd_fact_favor_info(
    `id`            string COMMENT '编号',
    `user_id`       string COMMENT '用户id',
    `sku_id`        string COMMENT 'skuid',
    `spu_id`        string COMMENT 'spuid',
    `is_cancel`     string COMMENT '是否取消',
    `create_time`   string COMMENT '收藏时间',
    `cancel_time`   string COMMENT '取消时间'
) COMMENT '收藏事实表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/dwd/dwd_fact_favor_info/';


insert overwrite table dwd_fact_favor_info partition (dt='$do_date')
select
    id            , -- 编
    user_id       , -- 用户id
    sku_id        , -- skuid
    spu_id        , -- spuid
    is_cancel     , -- 是否取消
    create_time   , -- 收藏时间
    cancel_time    -- 取消时间
from ods_favor_info
where dt = '$do_date';
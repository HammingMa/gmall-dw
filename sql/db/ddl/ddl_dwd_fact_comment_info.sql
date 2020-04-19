use gmall;

drop table if exists dwd_fact_comment_info;
create external table dwd_fact_comment_info(
    `id`            string COMMENT '编号',
    `user_id`       string COMMENT '用户ID',
    `sku_id`        string COMMENT '商品sku',
    `spu_id`        string COMMENT '商品spu',
    `order_id`      string COMMENT '订单ID',
    `appraise`      string COMMENT '评价',
    `create_time`   string COMMENT '评价时间'
) COMMENT '评价事实表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/dwd/dwd_fact_comment_info/';

insert overwrite table dwd_fact_comment_info partition (dt='$do_date')
select
    id            , -- 编号
    user_id       , -- 用户ID
    sku_id        , -- 商品sku
    spu_id        , -- 商品spu
    order_id      , -- 订单ID
    appraise      , -- 评价
    create_time     -- 评价时间
from ods_comment_info
where dt='$do_date';
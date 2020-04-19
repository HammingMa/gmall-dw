drop table if exists dwd_fact_order_refund_info;
create external table dwd_fact_order_refund_info(
    `id`                    string COMMENT '编号',
    `user_id`               string COMMENT '用户ID',
    `order_id`              string COMMENT '订单ID',
    `sku_id`                string COMMENT '商品ID',
    `refund_type`           string COMMENT '退款类型',
    `refund_num`            bigint COMMENT '退款件数',
    `refund_amount`         decimal(16,2) COMMENT '退款金额',
    `refund_reason_type`    string COMMENT '退款原因类型',
    `create_time`           string COMMENT '退款时间'
) COMMENT '退款事实表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/dwd/dwd_fact_order_refund_info/';


insert overwrite table dwd_fact_order_refund_info partition (dt='$do_date')
select
    id                    , -- 编号
    user_id               , -- 用户ID
    order_id              , -- 订单ID
    sku_id                , -- 商品ID
    refund_type           , -- 退款类型
    refund_num            , -- 退款件数
    refund_amount         , -- MMENT '退款金额
    refund_reason_type    , -- 退款原因类型
    create_time             -- 退款时间'
from ods_order_refund_info
where dt='$do_date';
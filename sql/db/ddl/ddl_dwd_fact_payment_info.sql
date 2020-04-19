use gmall;

drop table if exists dwd_fact_payment_info;
create external table dwd_fact_payment_info (
    `id`                string COMMENT '',
    `out_trade_no`      string COMMENT '对外业务编号',
    `order_id`          string COMMENT '订单编号',
    `user_id`           string COMMENT '用户编号',
    `alipay_trade_no`   string COMMENT '支付宝交易流水编号',
    `payment_amount`    decimal(16,2) COMMENT '支付金额',
    `subject`           string COMMENT '交易内容',
    `payment_type`      string COMMENT '支付类型',
    `payment_time`      string COMMENT '支付时间',
    `province_id`       string COMMENT '省份ID'
)comment '支付事实表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_payment_info/'
    tblproperties ("parquet.compression"="lzo");


insert overwrite table dwd_fact_payment_info partition (dt='$do_date')
select
    a.id              as id                , --
    a.out_trade_no    as out_trade_no      , -- 对外业务编号
    a.order_id        as order_id          , -- 订单编号
    a.user_id         as user_id           , -- 用户编号
    a.alipay_trade_no as alipay_trade_no   , -- 支付宝交易流水编号
    a.total_amount as payment_amount    , -- 支付金额
    a.subject         as subject           , -- 交易内容
    a.payment_type    as payment_type      , -- 支付类型
    a.payment_time    as payment_time      , -- 支付时间
    b.province_id     as province_id        -- 省份ID
from
    (select * from ods_payment_info  where dt='$do_date') a
        inner join
    (select * from ods_order_info  where dt='$do_date') b
    on a.order_id=b.id;
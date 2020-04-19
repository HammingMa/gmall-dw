use gmall;

drop table if exists dwd_fact_order_detail;
create external table dwd_fact_order_detail (
    `id`            string COMMENT '订单编号',
    `order_id`      string COMMENT '订单号',
    `user_id`       string COMMENT '用户id',
    `sku_id`        string COMMENT 'sku商品id',
    `sku_name`      string COMMENT '商品名称',
    `order_price`   decimal(10,2) COMMENT '商品价格',
    `sku_num`       bigint COMMENT '商品数量',
    `create_time`   string COMMENT '创建时间',
    `province_id`   string COMMENT '省份ID',
    `total_amount`  decimal(20,2) COMMENT '订单总金额'
)comment '订单明细详情事实表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_fact_order_detail/'
    tblproperties ("parquet.compression"="lzo");


insert overwrite table dwd_fact_order_detail partition (dt='$do_date')
select
    a.id           as id            , -- 订单编号
    a.order_id     as order_id      , -- 订单号
    a.user_id      as user_id       , -- 用户id
    a.sku_id       as sku_id        , -- sku商品id
    a.sku_name     as sku_name      , -- 商品名称
    a.order_price  as order_price   , -- 商品价格
    a.sku_num      as sku_num       , -- 商品数量
    a.create_time  as create_time   , -- 创建时间
    b.province_id  as province_id   , -- 省份ID
    a.order_price*a.sku_num as total_amount   -- 订单总金额
from
    (select * from ods_order_detail where dt='$do_date') a
inner join
    (select * from ods_order_info  where dt='$do_date') b
on a.order_id = b.id
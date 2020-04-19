use gmall;

drop table if exists dwd_fact_cart_info;
create external table dwd_fact_cart_info(
    `id`            string COMMENT '编号',
    `user_id`       string COMMENT '用户id',
    `sku_id`        string COMMENT 'skuid',
    `cart_price`    string COMMENT '放入购物车时价格',
    `sku_num`       string COMMENT '数量',
    `sku_name`      string COMMENT 'sku 名称 (冗余)',
    `create_time`   string COMMENT '创建时间',
    `operate_time`  string COMMENT '修改时间',
    `is_ordered`    string COMMENT '是否已经下单。1 为已下单,0 为未下单',
    `order_time`    string COMMENT '下单时间'
) COMMENT '加购事实表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/dwd/dwd_fact_cart_info/';

insert overwrite table dwd_fact_cart_info partition (dt='$do_date')
select
    id            , -- 编号
    user_id       , -- 用户id
    sku_id        , -- skuid
    cart_price    , -- 放入购物车时价格
    sku_num       , -- 数量
    sku_name      , -- sku 名称 (冗余)
    create_time   , -- 创建时间
    operate_time  , -- 修改时间
    is_ordered    , -- 是否已经下单。1 为已下单,0 为未下单
    order_time     -- 下单时间
from ods_cart_info
where dt='$do_date'
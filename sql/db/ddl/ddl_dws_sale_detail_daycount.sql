use gmall;

drop table if exists dws_sale_detail_daycount;
create external table dws_sale_detail_daycount
(
    user_id            string comment '用户 id',
    sku_id             string comment '商品 id',
    user_gender        string comment '用户性别',
    user_age           string comment '用户年龄',
    user_level         string comment '用户等级',
    order_price        decimal(10, 2) comment '商品价格',
    sku_namestring     string comment '商品名称',
    sku_tm_idstring    string comment '品牌id',
    sku_category3_id   string comment '商品三级品类 id',
    sku_category2_id   string comment '商品二级品类 id',
    sku_category1_id   string comment '商品一级品类 id',
    sku_category3_name string comment '商品三级品类名称',
    sku_category2_name string comment '商品二级品类名称',
    sku_category1_name string comment '商品一级品类名称',
    spu_id             string comment '商品 spu',
    sku_num            int comment '购买个数',
    order_count        bigint comment '当日下单单数',
    order_amount       decimal(16, 2) comment '当日下单金额'
) COMMENT '每日购买行为'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_sale_detail_daycount/'
    tblproperties ("parquet.compression" = "lzo");


with
tmp_order as (
    select user_id,
           sku_id,
           sum(sku_num)      as sku_num,
           count(*)          as order_count,
           sum(total_amount) as order_amount
    from dwd_fact_order_detail
    where dt = '$do_date'
    group by user_id, sku_id
),
tmp_user as (
    select *
    from dwd_dim_user_info_his
    where end_date = '9999-99-99'
),
tmp_sku as (
    select * from dwd_dim_sku_info where dt ='$do_date'
)
insert overwrite table dws_sale_detail_daycount partition (dt='$do_date')
select a.user_id,                                          -- 用户 id
       a.sku_id,                                           -- 商品 id
       b.gender,                                           -- 用户性别
       months_between('$do_date', b.birthday) / 12 as age, -- 用户年龄
       b.user_level,                                       -- 用户等级
       c.price,                                            -- 商品价格
       c.sku_name,                                         -- 商品名称
       c.tm_id,                                            -- 品牌id
       c.category3_id,                                     -- 商品三级品类 id
       c.category2_id,                                     -- 商品二级品类 id
       c.category1_id,                                     -- 商品一级品类 id
       c.category3_name,                                   -- 商品三级品类名称
       c.category2_name,                                   -- 商品二级品类名称
       c.category1_name,                                   -- 商品一级品类名称
       c.id,                                               -- 商品 spu
       a.sku_num,                                          -- 购买个数
       a.order_count,                                      -- 当日下单单数
       a.order_amount                                      -- 当日下单金额
from tmp_order a
         left join tmp_user b on a.user_id = b.id
         left join tmp_sku c on a.sku_id = c.id;

select * from dws_sale_detail_daycount;

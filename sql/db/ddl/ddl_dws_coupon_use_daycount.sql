use gmall;

drop table if exists dws_coupon_use_daycount;
create external table dws_coupon_use_daycount
(
    `coupon_id`        string COMMENT '优惠券 ID',
    `coupon_name`      string COMMENT '购物券名称',
    `coupon_type`      string COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` string COMMENT '满额数',
    `condition_num`    string COMMENT '满件数',
    `activity_id`      string COMMENT '活动编号',
    `benefit_amount`   string COMMENT '减金额',
    `benefit_discount` string COMMENT '折扣',
    `create_time`      string COMMENT '创建时间',
    `range_type`       string COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `spu_id`           string COMMENT '商品 id',
    `tm_id`            string COMMENT '品牌 id',
    `category3_id`     string COMMENT '品类 id',
    `limit_num`        string COMMENT '最多领用次数',
    `get_count`        bigint COMMENT '领用次数',
    `using_count`      bigint COMMENT '使用(下单)次数',
    `used_count`       bigint COMMENT '使用(支付)次数'
) COMMENT '每日优惠券统计'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dws/dws_coupon_use_daycount/'
    tblproperties ("parquet.compression" = "lzo");

with
tmp_cnt as (
    select coupon_id,
           sum(if(date_format(get_time, 'yyyy-MM-dd') = '$do_date', 1, 0))   as get_count,
           sum(if(date_format(using_time, 'yyyy-MM-dd') = '$do_date', 1, 0)) as using_count,
           sum(if(date_format(used_time, 'yyyy-MM-dd') = '$do_date', 1, 0))  as used_count
    from dwd_fact_coupon_use
    where dt = '$do_date'
    group by coupon_id
)
insert overwrite table dws_coupon_use_daycount partition (dt='$do_date')
select a.coupon_id,        -- 优惠券 ID
       b.coupon_name,      -- 购物券名称
       b.coupon_type,      -- 购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券
       b.condition_amount, -- 满额数
       b.condition_num,    -- 满件数
       b.activity_id,      -- 活动编号
       b.benefit_amount,   -- 减金额
       b.benefit_discount, -- 折扣
       b.create_time,      -- 创建时间
       b.range_type,       -- 范围类型 1、商品 2、品类 3、品牌
       b.spu_id,           -- 商品 id
       b.tm_id,            -- 品牌 id
       b.category3_id,     -- 品类 id
       b.limit_num,        -- 最多领用次数
       a.get_count,        -- 领用次数
       a.using_count,      -- 使用(下单)次数
       a.used_count        -- 使用(支付)次数
from tmp_cnt a
         left join (select * from dwd_dim_coupon_info where dt = '$do_date') b
                   on a.coupon_id = b.id;

select * from dws_coupon_use_daycount
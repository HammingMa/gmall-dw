use gmall;


drop table if exists dwd_dim_coupon_info;
create external table dwd_dim_coupon_info(
    `id`                string COMMENT '购物券编号',
    `coupon_name`       string COMMENT '购物券名称',
    `coupon_type`       string COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount`  string COMMENT '满额数',
    `condition_num`     string COMMENT '满件数',
    `activity_id`       string COMMENT '活动编号',
    `benefit_amount`    string COMMENT '减金额',
    `benefit_discount`  string COMMENT '折扣',
    `create_time`       string COMMENT '创建时间',
    `range_type`        string COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `spu_id`            string COMMENT '商品id',
    `tm_id`             string COMMENT '品牌id',
    `category3_id`      string COMMENT '品类id',
    `limit_num`         string COMMENT '最多领用次数',
    `operate_time`      string COMMENT '修改时间',
    `expire_time`       string COMMENT '过期时间'
) COMMENT '优惠券信息表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t'
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_coupon_info/'
    tblproperties ("parquet.compression"="lzo");

insert overwrite table dwd_dim_coupon_info partition (dt='$do_date')
select
    id                , --购物券编号
    coupon_name       , --购物券名称
    coupon_type       , --购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券
    condition_amount  , --满额数
    condition_num     , --满件数
    activity_id       , --活动编号
    benefit_amount    , --减金额
    benefit_discount  , --折扣
    create_time       , --创建时间
    range_type        , --范围类型 1、商品 2、品类 3、品牌
    spu_id            , --商品id
    tm_id             , --品牌id
    category3_id      , --品类id
    limit_num         , --最多领用次数
    operate_time      , --修改时间
    expire_time         --过期时间
from ods_coupon_info
where dt='$do_date';

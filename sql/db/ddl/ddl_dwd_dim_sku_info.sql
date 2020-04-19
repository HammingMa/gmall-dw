use gmall;


DROP TABLE IF EXISTS `dwd_dim_sku_info`;
CREATE EXTERNAL TABLE `dwd_dim_sku_info` (
    `id`                string COMMENT '商品id',
    `spu_id`            string COMMENT 'spuid',
    `price`             double COMMENT '商品价格',
    `sku_name`          string COMMENT '商品名称',
    `sku_desc`          string COMMENT '商品描述',
    `weight`            double COMMENT '重量',
    `tm_id`             string COMMENT '品牌id',
    `tm_name`           string COMMENT '品牌名称',
    `category3_id`      string COMMENT '三级分类 id',
    `category2_id`      string COMMENT '二级分类 id',
    `category1_id`      string COMMENT '一级分类 id',
    `category3_name`    string COMMENT '三级分类名称',
    `category2_name`    string COMMENT '二级分类名称',
    `category1_name`    string COMMENT '一级分类名称',
    `spu_name`          string COMMENT 'spu 名称',
    `create_time`       string COMMENT '创建时间'
)COMMENT '商品维度表'
    PARTITIONED BY (`dt` string)
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_sku_info/'
    tblproperties ("parquet.compression"="lzo");


insert into table dwd_dim_sku_info partition (dt='$do_date')
select
    a.`id`              as `id`                , --商品id
    a.`spu_id`          as `spu_id`            , --spuid
    a.`price`           as `price`             , --商品价格
    a.`sku_name`        as `sku_name`          , --商品名称
    a.`sku_desc`        as `sku_desc`          , --商品描述
    a.`weight`          as `weight`            , --重量
    a.`tm_id`           as `tm_id`             , --品牌id
    b.`tm_name`         as `tm_name`           , --品牌名称
    c.`category3_id`    as `category3_id`      , --三级分类 id
    d.`category2_id`    as `category2_id`      , --二级分类 id
    e.`category1_id`    as `category1_id`      , --一级分类 id
    d.`name`            as `category3_name`    , --三级分类名称
    e.`name`            as `category2_name`    , --二级分类名称
    f.`name`            as `category1_name`    , --一级分类名称
    c.`spu_name`        as `spu_name`          , --spu 名称
    a.`create_time`     as `create_time`        --创建时间
from
    (
        select
            *
        from ods_sku_info
        where dt='$do_date'
    ) a
inner join
    (
        select
            *
        from ods_base_trademark
        where dt='$do_date'
    ) b
on a.tm_id=b.tm_id
inner join
    (
        select
            *
        from ods_spu_info
        where dt='$do_date'
    ) c
on a.spu_id = c.id
inner join
    (
        select
            *
        from ods_base_category3
        where dt='$do_date'
    ) d
on a.category3_id=d.id
inner join
    (
        select
            *
        from ods_base_category2
        where dt='$do_date'
    ) e
    on d.category2_id=e.id
inner join
    (
        select
            *
        from ods_base_category2
        where dt='$do_date'
    ) f
    on e.category1_id=f.id;
use gmall;

DROP TABLE IF EXISTS `dwd_dim_base_province`;
CREATE EXTERNAL TABLE `dwd_dim_base_province` (
    `id`                string COMMENT 'id',
    `province_name`     string COMMENT '省市名称',
    `area_code`         string COMMENT '地区编码',
    `iso_code`          string COMMENT 'ISO 编码',
    `region_id`         string COMMENT '地区id',
    `region_name`       string COMMENT '地区名称'
)COMMENT '地区省市表'
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_base_province/'
    tblproperties ("parquet.compression"="lzo");


insert overwrite table dwd_dim_base_province
select
    a.id            as id                , -- id
    a.name          as province_name     , -- 省市名称
    a.area_code     as area_code         , -- 地区编码
    a.iso_code      as iso_code          , -- ISO 编码
    a.region_id     as region_id         , -- 地区id
    b.region_name   as region_name         -- 地区名称
from ods_base_province  a
    inner join ods_base_region b
    on  a.region_id =b.id;
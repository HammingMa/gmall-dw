use gmall;

drop table if exists ads_product_info;
create external table ads_product_info
(
    `dt`      string COMMENT '统计日期',
    `sku_num` string COMMENT 'sku 个数',
    `spu_num` string COMMENT 'spu 个数'
) COMMENT '商品个数信息'
    row format delimited fields terminated by '\t' 
    location '/warehouse/gmall/ads/ads_product_info';

insert into table ads_product_info
select '$do_date'   as dt,
       sum(sku_num) as sku_num,
       count(*)     as spu_num
from (
         select spu_id,
                count(*) as sku_num
         from dwt_sku_topic
         group by spu_id
     ) a;

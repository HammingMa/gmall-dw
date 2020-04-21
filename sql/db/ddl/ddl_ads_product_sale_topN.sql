use gmall;

drop table if exists ads_product_sale_topN;
create external table ads_product_sale_topN
(
    `dt`             string COMMENT '统计日期',
    `sku_id`         string COMMENT '商品 ID',
    `payment_amount` bigint COMMENT '销量',
    `rank_num`       bigint COMMENT '排名'
) COMMENT '商品个数信息'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_product_sale_topN';

insert into table ads_product_sale_topN
select *
from (select '$do_date'                                        as dt,             -- 统计日期
             sku_id                                            as sku_id,         -- 商品 ID
             payment_amount                                    as payment_amount, -- 销量
             row_number() over (order by payment_amount desc ) as rank_num        -- 排名
      from dws_sku_action_daycount
      where dt = '$do_date') a
where a.rank_num <= 10;
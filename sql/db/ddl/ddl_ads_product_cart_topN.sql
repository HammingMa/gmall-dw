use gmall;

drop table if exists ads_product_cart_topN;
create external table ads_product_cart_topN
(
    `dt`       string COMMENT '统计日期',
    `sku_id`   string COMMENT '商品 ID',
    `cart_num` bigint COMMENT '加入购物车数量',
    `rank_num` bigint COMMENT '排名'
) COMMENT '商品加入购物车 TopN'
    row format delimited fields terminated by '\t' location '/warehouse/gmall/ads/ads_product_cart_topN';



insert into table ads_product_cart_topN
select *
from (select '$do_date'                                  as dt,       -- 统计日期
             sku_id                                      as sku_id,   -- 商品 ID
             cart_num                                    as cart_num, -- 加入购物车数量
             row_number() over (order by cart_num desc ) as rank_num  -- 排名
      from dws_sku_action_daycount
      where dt = '$do_date') a
where a.rank_num <= 10;
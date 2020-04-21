use gmall;

drop table if exists ads_product_favor_topN;
create external table ads_product_favor_topN
(
    `dt`          string COMMENT '统计日期',
    `sku_id`      string COMMENT '商品 ID',
    `favor_count` bigint COMMENT '收藏量',
    `rank_num`    bigint COMMENT '排名'
) COMMENT '商品收藏 TopN'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_product_favor_topN';


insert into table ads_product_favor_topN
select *
from (select '$do_date'                                     as dt,          -- 统计日期
             sku_id                                         as sku_id,      -- 商品 ID
             favor_count                                    as favor_count, -- 收藏量
             row_number() over (order by favor_count desc ) as rank_num     -- 排名
      from dws_sku_action_daycount
      where dt = '$do_date') a
where a.rank_num <= 10;


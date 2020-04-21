use gmall;

drop table if exists ads_product_refund_topN;
create external table ads_product_refund_topN
(
    `dt`                     string COMMENT '统计日期',
    `sku_id`                 string COMMENT '商品 ID',
    `payment_last_30d_count` bigint COMMENT '30天支付数据量',
    `refund_last_30d_count`  bigint COMMENT '30天退款数据量',
    `refund_ratio`           decimal(10, 2) COMMENT '退款率',
    `rank_num`               bigint COMMENT '排名'
) COMMENT '商品退款率 TopN'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_product_refund_topN';


insert into table ads_product_refund_topN
select *
from (select '$do_date'                                      as dt,                     -- 统计日期
             sku_id                                          as sku_id,                 -- 商品 ID
             payment_last_30d_count                          as payment_last_30d_count, -- 30天支付数据量
             refund_last_30d_count                           as refund_last_30d_count,  -- 30天退款数据量
             refund_ratio                                    as refund_ratio,           -- 退款率
             row_number() over (order by refund_ratio desc ) as rank_num                -- 排名
      from (
               select sku_id,                                                              -- 商品 ID
                      payment_last_30d_count,                                              -- 30天支付数据量
                      refund_last_30d_count,                                               -- 30天退款数据量
                      refund_last_30d_count / payment_last_30d_count * 100 as refund_ratio -- 退款率
               from dwt_sku_topic
           ) b ) a
where a.rank_num <= 10;
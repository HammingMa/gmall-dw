use gmall;

drop table if exists ads_appraise_bad_topN;
create external table ads_appraise_bad_topN
(
    dt                     string COMMENT '统计日期',
    sku_id                 string COMMENT '商品 ID',
    appraise_good_count    bigint comment '好评数',
    appraise_mid_count     bigint comment '中评数',
    appraise_bad_count     bigint comment '差评数',
    appraise_default_count bigint comment '默认评价数',
    appraise_count         bigint comment '总评价书',
    appraise_bad_ratio     decimal(10, 2) COMMENT '差评率',
    rank_num               bigint COMMENT '排名'
) COMMENT '商品差评率 TopN'
    row format delimited fields terminated by '\t' location '/warehouse/gmall/ads/ads_appraise_bad_topN';


insert into table ads_appraise_bad_topN
select *
from (select '$do_date'                                            as dt,                     -- 统计日期
             sku_id                                                as sku_id,                 -- 商品 ID
             appraise_good_count                                   as appraise_good_count,    -- 好评数
             appraise_mid_count                                    as appraise_mid_count,     -- 中评数
             appraise_bad_count                                    as appraise_bad_count,     -- 差评数
             appraise_default_count                                as appraise_default_count, -- 默认评价数
             appraise_count                                        as appraise_count,         -- 总评价书
             appraise_bad_ratio                                    as appraise_bad_ratio,     -- 差评率
             row_number() over (order by appraise_bad_ratio desc ) as rank_num                -- 排名
      from (
               select sku_id                                        as sku_id,                 -- 商品 ID
                      appraise_good_count                           as appraise_good_count,    -- 好评数
                      appraise_mid_count                            as appraise_mid_count,     -- 中评数
                      appraise_bad_count                            as appraise_bad_count,     -- 差评数
                      appraise_default_count                        as appraise_default_count, -- 默认评价数
                      appraise_good_count + appraise_mid_count + appraise_bad_count +
                      appraise_default_count                        as appraise_count,         -- 总评价书
                      appraise_bad_count / (appraise_good_count + appraise_mid_count + appraise_bad_count +
                                            appraise_default_count) as appraise_bad_ratio      -- 差评率
               from dws_sku_action_daycount
               where dt = '$do_date'
           ) b) a
where a.rank_num <= 10;
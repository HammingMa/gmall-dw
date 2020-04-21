use gmall;

drop table ads_sale_tm_category1_stat_mn;
create external table ads_sale_tm_category1_stat_mn
(
    tm_id                 string comment '品牌 id',
    category1_id          string comment '1级品类id ',
    category1_name        string comment '1 级品类名称 ',
    buycount              bigint comment '购买人数',
    buy_twice_last        bigint comment '两次以上购买人数',
    buy_twice_last_ratio  decimal(10, 2) comment '单次复购率',
    buy_3times_last       bigint comment '三次以上购买人数',
    buy_3times_last_ratio decimal(10, 2) comment '多次复购率',
    stat_mn               string comment '统计月份',
    stat_date             string comment '统计日期'
) COMMENT '复购率统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_sale_tm_category1_stat_mn/';

insert overwrite table ads_sale_tm_category1_stat_mn
select tm_id,-- 品牌 id',
       category1_id,-- 1级品类id ',
       category1_name,-- 1 级品类名称 ',
       buycount,-- 购买人数',
       buy_twice_last,-- 两次以上购买人数',
       buy_twice_last / buycount          as buy_twice_last_ratio,-- 单次复购率',
       buy_3times_last,-- 三次以上购买人数',
       buy_3times_last / buycount         as buy_3times_last_ratio,-- 多次复购率',
       date_format('$do_date', 'yyyy-MM') as stat_mn, -- 统计月份
       '$do_date'                         as stat_date -- 统计日期
from (
         select sku_tm_id                      as tm_id,          -- 品牌 id
                sku_category1_id               as category1_id,   -- 1级品类id
                sku_category1_name             as category1_name, -- 1 级品类名称
                sum(if(order_count > 0, 1, 0)) as buycount,       -- 购买人数
                sum(if(order_count > 0, 1, 0)) as buy_twice_last, -- 两次以上购买人数
                sum(if(order_count > 0, 1, 0)) as buy_3times_last -- 三次以上购买人数
         from (
                  select user_id,
                         sku_tm_id,                      -- 品牌 id
                         sku_category1_id,               -- 1级品类id
                         sku_category1_name,             -- 1 级品类名称
                         sum(order_count) as order_count -- 购买次数
                  from dws_sale_detail_daycount
                  where date_format(dt, 'yyyy-MM') = date_format('$do_date', 'yyyy-MM')
                  group by user_id, sku_tm_id, sku_category1_id, sku_category1_name
              ) a
         group by sku_tm_id, sku_category1_id, sku_category1_name, date_format('$do_date', 'yyyy-MM'), '$do_date'
     ) b;
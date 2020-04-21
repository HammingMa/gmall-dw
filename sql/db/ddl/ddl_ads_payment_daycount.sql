use gmall;


drop table if exists ads_payment_daycount;
create external table ads_payment_daycount
(
    dt                 string comment '统计日期',
    order_count        bigint comment '单日支付笔数',
    order_amount       bigint comment '单日支付金额',
    payment_user_count bigint comment '单日支付人数',
    payment_sku_count  bigint comment '单日支付商品数',
    payment_avg_time   double comment '下单到支付的平均时长，取分钟数'
) comment '每日订单总计表'
    row format delimited fields terminated by '\t' location '/warehouse/gmall/ads/ads_payment_daycount';

with
tmp_payment as (
    select dt,
           sum(payment_count)               as order_count,       -- 单日支付笔数
           sum(payment_amount)              as order_amount,      -- 单日支付金额
           sum(if(payment_count > 0, 1, 0)) as payment_user_count -- 单日支付人数
    from dws_user_action_daycount
    where dt = '$do_date'
    group by dt
),
tmp_sku as (
    select dt,
           sum(if(payment_count > 0, 1, 0)) payment_sku_count
    from dws_sku_action_daycount
    where dt = '$do_date'
    group by dt
),
tmp_time as (
    select dt,
           sum(unix_timestamp(payment_time) - unix_timestamp(create_time)) / count(*) / 60 as payment_avg_time
    from dwd_fact_order_info
    where dt = '$do_date'
      and payment_time is not null
    group by dt
)
insert into table ads_payment_daycount
select '$do_date' as dt,-- 统计日期
       order_count,-- 单日支付笔数
       order_amount,-- 单日支付金额
       payment_user_count,-- 单日支付人数
       payment_sku_count,-- 单日支付商品数
       payment_avg_time -- 下单到支付的平均时长，取分钟数
from tmp_payment a
         left join tmp_sku b on a.dt = b.dt
         left join tmp_time c on a.dt = c.dt

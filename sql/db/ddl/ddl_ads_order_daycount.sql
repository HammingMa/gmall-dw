use gmall;

drop table if exists ads_order_daycount;
create external table ads_order_daycount
(
    dt           string comment '统计日期',
    order_count  bigint comment '单日下单笔数',
    order_amount bigint comment '单日下单金额',
    order_users  bigint comment '单日下单用户数'
) comment '每日订单总计表'
    row format delimited fields terminated by '\t' location '/warehouse/gmall/ads/ads_order_daycount';



insert into table ads_order_daycount
select '$do_date'                     as dt,           -- 统计日期
       sum(order_count)               as order_count,  -- 单日下单笔数
       sum(order_amount)              as order_amount, -- 单日下单金额
       sum(if(order_count > 0, 1, 0)) as order_users   -- 单日下单用户数
from dws_user_action_daycount
where dt = '$do_date';
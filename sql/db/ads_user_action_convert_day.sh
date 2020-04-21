#!/bin/bash

if [ -n $1 ]; then
    do_date=$1
else
    do_date=`date -d '-1 day' +%F`
fi

echo $do_date

db=gmall
hive=/opt/hive/bin/hive
hadoop=/opt/hadoop/bin/hadoop

sql="
set hive.exec.dynamic.partition.mode=nonstrict;
use $db;

with
tmp_user_order as (
    select dt,
           sum(if(cart_count > 0, 1, 0))    as cart_u_count,
           sum(if(order_count > 0, 1, 0))   as order_u_count,
           sum(if(payment_count > 0, 1, 0)) as payment_u_count
    from dws_user_action_daycount
    where dt = '$do_date'
    group by dt
)
insert into table ads_user_action_convert_day
select a.dt,                                                              -- 统计日期
       b.day_count                         as total_visitor_m_count,      -- 总访问人数
       a.cart_u_count                      as cart_u_count,               -- 加入购物车的人数
       a.cart_u_count / b.day_count        as visitor2cart_convert_ratio, -- 访问到加入购物车转化率
       a.order_u_count                     as order_u_count,              -- 下单人数
       a.order_u_count / a.cart_u_count    as cart2order_convert_ratio,   -- 加入购物车到下单转化率
       a.payment_u_count                   as payment_u_count,            -- 支付人数
       a.payment_u_count / a.order_u_count as order2payment_convert_ratio --下单到支付的转化率
from tmp_user_order a
         left join ads_uv_count b on a.dt = b.dt;

"

## 加载数据
$hive -e "$sql"

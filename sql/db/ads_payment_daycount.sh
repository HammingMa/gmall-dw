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
         left join tmp_time c on a.dt = c.dt;
"

## 加载数据
$hive -e "$sql"

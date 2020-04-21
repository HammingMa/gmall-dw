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
tmp_log as (
    select
        user_id,
        count(*) as login_count
    from dwd_start_log
    where dt='$do_date'
    group by user_id
),
tmp_cart as (
    select
        user_id,
        count(*) as cart_count,
        sum(cart_price*sku_num) as cart_amount
    from dwd_fact_cart_info
    where dt='$do_date'
        and user_id is not null
        and date_format(create_time,'yyyy-MM-dd')='$do_date'
    group by user_id
),
tmp_order as (
    select
        user_id,
        count(*) as order_count,
        sum(final_total_amount) as order_amount
    from dwd_fact_order_info
    where dt='$do_date'
    group by user_id
),
tmp_payment as (
    select
        user_id,
        count(*) as payment_count,
        sum(payment_amount) as payment_amount
    from dwd_fact_payment_info
    where dt='$do_date'
    group by user_id
)

insert overwrite table dws_user_action_daycount partition (dt='$do_date')
select
    user_id         , -- 用户 id
    sum(login_count   ) as login_count     , -- 登录次数
    sum(cart_count    ) as cart_count      , -- 加入购物车次数
    sum(cart_amount   ) as cart_amount     , -- 加入购物车金额
    sum(order_count   ) as order_count     , -- 下单次数
    sum(order_amount  ) as order_amount    , -- 下单金额
    sum(payment_count ) as payment_count   , -- 支付次数
    sum(payment_amount) as payment_amount   -- 支付金额
from
(
    select
        user_id         , -- 用户 id
        login_count     , -- 登录次数
        0 as cart_count      , -- 加入购物车次数
        0 as cart_amount     , -- 加入购物车金额
        0 as order_count     , -- 下单次数
        0 as order_amount    , -- 下单金额
        0 as payment_count   , -- 支付次数
        0 as payment_amount   -- 支付金额
    from tmp_log
    union all
    select
        user_id         , -- 用户 id
        0 as login_count     , -- 登录次数
        cart_count      , -- 加入购物车次数
        cart_amount     , -- 加入购物车金额
        0 as order_count     , -- 下单次数
        0 as order_amount    , --下单金额
        0 as payment_count   , -- 支付次数
        0 as payment_amount   -- 支付金额
    from tmp_cart
    union all
    select
        user_id         , -- 用户 id
        0 as login_count     , -- 登录次数
        0 as cart_count      , -- 加入购物车次数
        0 as cart_amount     , -- 加入购物车金额
        order_count     , -- 下单次数
        order_amount    , -- 下单金额
        0 as payment_count   , -- 支付次数
        0 as payment_amount   -- 支付金额
    from tmp_order
    union all
    select
        user_id         , -- 用户 id
        0 as login_count     , -- 登录次数
        0 as cart_count      , -- 加入购物车次数
        0 as cart_amount     , -- 加入购物车金额
        0 as order_count     , -- 下单次数
        0 as order_amount    , -- 下单金额
        payment_count   , -- 支付次数
        payment_amount   -- 支付金额
    from tmp_payment
    ) a
group by user_id;

"

## 加载数据
$hive -e "$sql"

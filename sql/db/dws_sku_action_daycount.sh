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
tmp_order as (
    select
        sku_id,
        count(*) as order_count,
        sum(sku_num) as order_num,
        sum(total_amount) as order_amount
    from dwd_fact_order_detail
    where dt='$do_date'
    group by sku_id
),
tmp_payment as (
    select
        sku_id,
        count(*) as payment_count,
        sum(sku_num) as payment_num,
        sum(total_amount) as payment_amount
    from dwd_fact_order_detail
    where (dt='$do_date' or dt=date_add('$do_date',-1))
        and order_id in (
            select order_id
                from dwd_fact_payment_info
            where dt='$do_date'
        )
    group by sku_id
),
tmp_refund as (
    select
        sku_id,
        count(*) as refund_count,
        sum(refund_num) as refund_num,
        sum(refund_amount) as refund_amount
    from dwd_fact_order_refund_info
    where dt = '$do_date'
    group by sku_id
),
tmp_cart as (
    select
        sku_id,
        count(*) as cart_count,
        sum(sku_num) cart_num
    from dwd_fact_cart_info
    where dt='$do_date'
    group by sku_id
),
tmp_favor as (
    select
        sku_id,
        count(*) as favor_count
    from dwd_fact_favor_info
    where dt='$do_date'
    group by sku_id
),
tmp_appraise as (
    select
        sku_id,
        sum(if(appraise='1201',1,0)) as appraise_good_count,
        sum(if(appraise='1202',1,0)) as appraise_mid_count,
        sum(if(appraise='1203',1,0)) as appraise_bad_count,
        sum(if(appraise='1204',1,0)) as appraise_default_count
    from dwd_fact_comment_info
    where dt='$do_date'
    group by sku_id
)
insert overwrite table dws_sku_action_daycount partition (dt='$do_date')
select
    sku_id                 , -- sku_id
    sum(order_count           ) , -- 被下单次数
    sum(order_num             ) , -- 被下单件数
    sum(order_amount          ) , -- omment '被下单金额
    sum(payment_count         ) , -- 被支付次数
    sum(payment_num           ) , -- 被支付件数
    sum(payment_amount        ) , -- omment '被支付金额
    sum(refund_count          ) , -- 被退款次数
    sum(refund_num            ) , -- 被退款件数
    sum(refund_amount         ) , -- omment '被退款金额
    sum(cart_count            ) , -- 被加入购物车次数
    sum(cart_num              ) , -- 被加入购物车件数
    sum(favor_count           ) , -- 被收藏次数
    sum(appraise_good_count   ) , -- 好评数
    sum(appraise_mid_count    ) , -- 中评数
    sum(appraise_bad_count    ) , -- 差评数
    sum(appraise_default_count)   -- 默认评价数
from (
     select
         sku_id                 , -- sku_id
         order_count            , -- 被下单次数
         order_num              , -- 被下单件数
         order_amount           , -- omment '被下单金额
         0 as payment_count          , -- 被支付次数
         0 as payment_num            , -- 被支付件数
         0 as payment_amount         , -- omment '被支付金额
         0 as refund_count           , -- 被退款次数
         0 as refund_num             , -- 被退款件数
         0 as refund_amount          , -- omment '被退款金额
         0 as cart_count             , -- 被加入购物车次数
         0 as cart_num               , -- 被加入购物车件数
         0 as favor_count            , -- 被收藏次数
         0 as appraise_good_count    , -- 好评数
         0 as appraise_mid_count     , -- 中评数
         0 as appraise_bad_count     , -- 差评数
         0 as appraise_default_count   -- 默认评价数
    from tmp_order
    union all
     select
         sku_id                 , -- sku_id
         0 as order_count            , -- 被下单次数
         0 as order_num              , -- 被下单件数
         0 as order_amount           , -- omment '被下单金额
         payment_count          , -- 被支付次数
         payment_num            , -- 被支付件数
         payment_amount         , -- omment '被支付金额
         0 as refund_count           , -- 被退款次数
         0 as refund_num             , -- 被退款件数
         0 as refund_amount          , -- omment '被退款金额
         0 as cart_count             , -- 被加入购物车次数
         0 as cart_num               , -- 被加入购物车件数
         0 as favor_count            , -- 被收藏次数
         0 as appraise_good_count    , -- 好评数
         0 as appraise_mid_count     , -- 中评数
         0 as appraise_bad_count     , -- 差评数
         0 as appraise_default_count   -- 默认评价数
     from tmp_payment
     union all
     select
         sku_id                 , -- sku_id
         0 as order_count            , -- 被下单次数
         0 as order_num              , -- 被下单件数
         0 as order_amount           , -- omment '被下单金额
         0 as payment_count          , -- 被支付次数
         0 as payment_num            , -- 被支付件数
         0 as payment_amount         , -- omment '被支付金额
         refund_count           , -- 被退款次数
         refund_num             , -- 被退款件数
         refund_amount          , -- omment '被退款金额
         0 as cart_count             , -- 被加入购物车次数
         0 as cart_num               , -- 被加入购物车件数
         0 as favor_count            , -- 被收藏次数
         0 as appraise_good_count    , -- 好评数
         0 as appraise_mid_count     , -- 中评数
         0 as appraise_bad_count     , -- 差评数
         0 as appraise_default_count   -- 默认评价数
     from tmp_refund
     union all
     select
         sku_id                 , -- sku_id
         0 as order_count            , -- 被下单次数
         0 as order_num              , -- 被下单件数
         0 as order_amount           , -- omment '被下单金额
         0 as payment_count          , -- 被支付次数
         0 as payment_num            , -- 被支付件数
         0 as payment_amount         , -- omment '被支付金额
         0 as refund_count           , -- 被退款次数
         0 as refund_num             , -- 被退款件数
         0 as refund_amount          , -- omment '被退款金额
         cart_count             , -- 被加入购物车次数
         cart_num               , -- 被加入购物车件数
         0 as favor_count            , -- 被收藏次数
         0 as appraise_good_count    , -- 好评数
         0 as appraise_mid_count     , -- 中评数
         0 as appraise_bad_count     , -- 差评数
         0 as appraise_default_count   -- 默认评价数
     from tmp_cart
     union all
     select
         sku_id                 , -- sku_id
         0 as order_count            , -- 被下单次数
         0 as order_num              , -- 被下单件数
         0 as order_amount           , -- omment '被下单金额
         0 as payment_count          , -- 被支付次数
         0 as payment_num            , -- 被支付件数
         0 as payment_amount         , -- omment '被支付金额
         0 as refund_count           , -- 被退款次数
         0 as refund_num             , -- 被退款件数
         0 as refund_amount          , -- omment '被退款金额
         0 as cart_count             , -- 被加入购物车次数
         0 as cart_num               , -- 被加入购物车件数
         favor_count            , -- 被收藏次数
         0 as appraise_good_count    , -- 好评数
         0 as appraise_mid_count     , -- 中评数
         0 as appraise_bad_count     , -- 差评数
         0 as appraise_default_count   -- 默认评价数
     from tmp_favor
     union all
     select
         sku_id                 , -- sku_id
         0 as order_count            , -- 被下单次数
         0 as order_num              , -- 被下单件数
         0 as order_amount           , -- omment '被下单金额
         0 as payment_count          , -- 被支付次数
         0 as payment_num            , -- 被支付件数
         0 as payment_amount         , -- omment '被支付金额
         0 as refund_count           , -- 被退款次数
         0 as refund_num             , -- 被退款件数
         0 as refund_amount          , -- omment '被退款金额
         0 as cart_count             , -- 被加入购物车次数
         0 as cart_num               , -- 被加入购物车件数
         0 as favor_count            , -- 被收藏次数
         appraise_good_count    , -- 好评数
         appraise_mid_count     , -- 中评数
         appraise_bad_count     , -- 差评数
         appraise_default_count   -- 默认评价数
     from tmp_appraise
         ) a
group by sku_id;

"

## 加载数据
$hive -e "$sql"

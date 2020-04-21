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
tmp_new as (
    select a.sku_id,
           b.spu_id,
           sum(if(a.dt = '$do_date', order_count, 0))            as order_count,
           sum(if(a.dt = '$do_date', order_num, 0))              as order_num,
           sum(if(a.dt = '$do_date', order_amount, 0))           as order_amount,
           sum(if(a.dt = '$do_date', payment_count, 0))          as payment_count,
           sum(if(a.dt = '$do_date', payment_num, 0))            as payment_num,
           sum(if(a.dt = '$do_date', payment_amount, 0))         as payment_amount,
           sum(if(a.dt = '$do_date', refund_count, 0))           as refund_count,
           sum(if(a.dt = '$do_date', refund_num, 0))             as refund_num,
           sum(if(a.dt = '$do_date', refund_amount, 0))          as refund_amount,
           sum(if(a.dt = '$do_date', cart_count, 0))             as cart_count,
           sum(if(a.dt = '$do_date', cart_num, 0))               as cart_num,
           sum(if(a.dt = '$do_date', favor_count, 0))            as favor_count,
           sum(if(a.dt = '$do_date', appraise_good_count, 0))    as appraise_good_count,
           sum(if(a.dt = '$do_date', appraise_mid_count, 0))     as appraise_mid_count,
           sum(if(a.dt = '$do_date', appraise_bad_count, 0))     as appraise_bad_count,
           sum(if(a.dt = '$do_date', appraise_default_count, 0)) as appraise_default_count,
           sum(order_count)                                    as order_count30,
           sum(order_num)                                      as order_num30,
           sum(order_amount)                                   as order_amount30,
           sum(payment_count)                                  as payment_count30,
           sum(payment_num)                                    as payment_num30,
           sum(payment_amount)                                 as payment_amount30,
           sum(refund_count)                                   as refund_count30,
           sum(refund_num)                                     as refund_num30,
           sum(refund_amount)                                  as refund_amount30,
           sum(cart_count)                                     as cart_count30,
           sum(cart_num)                                       as cart_num30,
           sum(favor_count)                                    as favor_count30,
           sum(appraise_good_count)                            as appraise_good_count30,
           sum(appraise_mid_count)                             as appraise_mid_count30,
           sum(appraise_bad_count)                             as appraise_bad_count30,
           sum(appraise_default_count)                         as appraise_default_count30
    from (select *
          from dws_sku_action_daycount
          where dt >= date_add('$do_date', -30)) a
             left join
             (select * from dwd_dim_sku_info where dt = '$do_date') b
             on a.sku_id = b.id
    group by sku_id,spu_id
)
insert overwrite table dwt_sku_topic
select nvl(b.sku_id, a.sku_id)                                             as sku_id,                          -- sku_id
       nvl(b.spu_id, a.spu_id)                                             as spu_id,                          -- spu_id
       nvl(b.order_count30, 0)                                              as order_last_30d_count,            -- 最近30日被下单次数
       nvl(b.order_num30, 0)                                               as order_last_30d_num,              -- 最近30日被下单件数
       nvl(b.order_amount30, 0)                                            as order_last_30d_amount,           -- 最近30日被下单金额
       nvl(a.order_count, 0) + nvl(b.order_count, 0)                       as order_count,                     -- 累积被下单次数
       nvl(a.order_num, 0) + nvl(b.order_num, 0)                           as order_num,                       -- 累积被下单件数
       nvl(a.order_amount, 0) + nvl(b.order_amount, 0)                     as order_amount,                    -- 累积被下单金额
       nvl(b.payment_count30, 0)                                           as payment_last_30d_count,          -- 最近30日被支付次数
       nvl(b.payment_num30, 0)                                             as payment_last_30d_num,            -- 最近30日被支付件数
       nvl(b.payment_amount30, 0)                                          as payment_last_30d_amount,         -- 最近30日被支付金额
       nvl(a.payment_count, 0) + nvl(b.payment_count, 0)                   as payment_count,                   -- 累积被支付次数
       nvl(a.payment_num, 0) + nvl(b.payment_num, 0)                       as payment_num,                     -- 累积被支付件数
       nvl(a.payment_amount, 0) + nvl(b.payment_amount, 0)                 as payment_amount,                  -- 累积被支付金额
       nvl(b.refund_count30, 0)                                            as refund_last_30d_count,           -- 最近三十日退款次数
       nvl(b.refund_num30, 0)                                              as refund_last_30d_num,             -- 最近三十日退款件数
       nvl(b.refund_amount30, 0)                                           as refund_last_30d_amount,          -- 最近三十日退款金额
       nvl(a.refund_count, 0) + nvl(b.refund_count, 0)                     as refund_count,                    -- 累积退款次数
       nvl(a.refund_count, 0) + nvl(b.refund_count, 0)                     as refund_num,                      -- 累积退款件数
       nvl(a.refund_count, 0) + nvl(b.refund_count, 0)                     as refund_amount,                   -- 累积退款金额
       nvl(b.cart_count30, 0)                                              as cart_last_30d_count,             -- 最近30日被加入购物车次数
       nvl(b.cart_num30, 0)                                                as cart_last_30d_num,               -- 最近30日被加入购物车件数
       nvl(a.cart_count, 0) + nvl(b.cart_count, 0)                         as cart_count,                      -- 累积被加入购物车次数
       nvl(a.cart_num, 0) + nvl(b.cart_num, 0)                             as cart_num,                        -- 累积被加入购物车件数
       nvl(b.favor_count30, 0)                                             as favor_last_30d_count,            -- 最近30日被收藏次数
       nvl(a.favor_count, 0) + nvl(b.favor_count, 0)                       as favor_count,                     -- 累积被收藏次数
       nvl(b.appraise_good_count30, 0)                                     as appraise_last_30d_good_count,    -- 最近30日好评数
       nvl(b.appraise_mid_count30, 0)                                      as appraise_last_30d_mid_count,     -- 最近30日中评数
       nvl(b.appraise_bad_count30, 0)                                      as appraise_last_30d_bad_count,     -- 最近30日差评数
       nvl(b.appraise_default_count30, 0)                                  as appraise_last_30d_default_count, -- 最近30日默认评价数
       nvl(a.appraise_good_count, 0) + nvl(b.appraise_good_count, 0)       as appraise_good_count,             -- 累积好评数
       nvl(a.appraise_mid_count, 0) + nvl(b.appraise_mid_count, 0)         as appraise_mid_count,              -- 累积中评数
       nvl(a.appraise_bad_count, 0) + nvl(b.appraise_bad_count, 0)         as appraise_bad_count,              -- 累积差评数
       nvl(a.appraise_default_count, 0) + nvl(b.appraise_default_count, 0) as appraise_default_count           -- 累积默认评价数
from dwt_sku_topic a
         full outer join tmp_new b
                         on a.sku_id = b.sku_id;

"

## 加载数据
$hive -e "$sql"

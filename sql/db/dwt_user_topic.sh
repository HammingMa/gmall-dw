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
         select
            user_id,
            sum(if(dt='$do_date',login_count,0)) as login_count,
            sum(if(dt='$do_date',order_count,0)) as order_count,
            sum(if(dt='$do_date',order_amount,0)) as order_amount,
            sum(if(dt='$do_date',payment_count,0)) as payment_count,
            sum(if(dt='$do_date',payment_amount,0)) as payment_amount,
            sum(if(login_count>0,1,0)) as login_last_30d_count,
            sum(order_count) as order_last_30d_count,
            sum(order_amount) as order_last_30d_amount,
            sum(payment_count) as payment_last_30d_count,
            sum(payment_amount) as payment_last_30d_amount
         from dws_user_action_daycount where dt>= date_add('$do_date',-30)
         group by user_id
     )
insert overwrite table dwt_user_topic
select coalesce(b.user_id, a.user_id)                                 as user_id,                -- 用户 id
       if(a.login_date_first is null and b.login_count > 0, '$do_date',
          a.login_date_first)                                         as login_date_first,       -- 首次登录时间
       coalesce(if(b.login_count > 0, '$do_date', a.login_date_last)) as login_date_last,        -- 末次登录时间
       nvl(a.login_count, 0) + if(b.login_count > 0, 1, 0)            as login_count,            -- 累积登录天数
       nvl(b.login_last_30d_count, 0)                                 as login_last_30d_count,   -- 最近30日登录天数
       if(a.order_date_first is null and b.order_count > 0, '$do_date',
          a.order_date_first)                                         as order_date_first,       -- 首次下单时间
       if(b.order_count > 0, '$do_date', a.order_date_last)           as order_date_last,        -- 末次下单时间
       coalesce(a.order_count, 0) + coalesce(b.order_count, 0)        as order_count,            -- 累积下单次数
       coalesce(a.order_amount, 0) + coalesce(b.order_amount, 0)      as order_amount,           -- 累积下单金额
       coalesce(b.order_last_30d_count, 0)                            as order_last_30d_count,   -- 最近30日下单次数
       coalesce(b.order_last_30d_amount, 0)                           as order_last_30d_amount,  -- 最近30日下单金额
       if(a.payment_date_first is null and b.payment_count > 0, '$do_date',
          a.payment_date_first)                                       as payment_date_first,     -- 首次支付时间
       if(b.payment_count > 0, '$do_date', a.payment_date_last)       as payment_date_last,      -- 末次支付时间
       coalesce(a.payment_count, 0) + coalesce(b.payment_count, 0)    as payment_count,          -- 累积支付次数
       coalesce(a.payment_amount, 0) + coalesce(b.payment_amount, 0)  as payment_amount,         -- 累积支付金额
       coalesce(b.payment_last_30d_count, 0)                          as payment_last_30d_count, -- 最近30日支付次数
       coalesce(b.payment_last_30d_amount, 0)                         as payment_last_30d_amount -- 最近30日支付金额
from dwt_user_topic a
         full outer join tmp_new b
                         on a.user_id = b.user_id;

"

## 加载数据
$hive -e "$sql"

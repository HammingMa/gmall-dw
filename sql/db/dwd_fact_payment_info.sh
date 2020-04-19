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

insert overwrite table dwd_fact_payment_info partition (dt='$do_date')
select
    a.id              as id                , --
    a.out_trade_no    as out_trade_no      , -- 对外业务编号
    a.order_id        as order_id          , -- 订单编号
    a.user_id         as user_id           , -- 用户编号
    a.alipay_trade_no as alipay_trade_no   , -- 支付宝交易流水编号
    a.total_amount as payment_amount    , -- 支付金额
    a.subject         as subject           , -- 交易内容
    a.payment_type    as payment_type      , -- 支付类型
    a.payment_time    as payment_time      , -- 支付时间
    b.province_id     as province_id        -- 省份ID
from
    (select * from ods_payment_info  where dt='$do_date') a
        inner join
    (select * from ods_order_info  where dt='$do_date') b
    on a.order_id=b.id;

"

## 加载数据
$hive -e "$sql"

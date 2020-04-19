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

insert overwrite table dwd_fact_order_refund_info partition (dt='$do_date')
select
    id                    , -- 编号
    user_id               , -- 用户ID
    order_id              , -- 订单ID
    sku_id                , -- 商品ID
    refund_type           , -- 退款类型
    refund_num            , -- 退款件数
    refund_amount         , -- MMENT '退款金额
    refund_reason_type    , -- 退款原因类型
    create_time             -- 退款时间'
from ods_order_refund_info
where dt='$do_date';
"

## 加载数据
$hive -e "$sql"

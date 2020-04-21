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

insert into table ads_product_refund_topN
select *
from (select '$do_date'                                      as dt,                     -- 统计日期
             sku_id                                          as sku_id,                 -- 商品 ID
             payment_last_30d_count                          as payment_last_30d_count, -- 30天支付数据量
             refund_last_30d_count                           as refund_last_30d_count,  -- 30天退款数据量
             refund_ratio                                    as refund_ratio,           -- 退款率
             row_number() over (order by refund_ratio desc ) as rank_num                -- 排名
      from (
               select sku_id,                                                              -- 商品 ID
                      payment_last_30d_count,                                              -- 30天支付数据量
                      refund_last_30d_count,                                               -- 30天退款数据量
                      refund_last_30d_count / payment_last_30d_count * 100 as refund_ratio -- 退款率
               from dwt_sku_topic
           ) b ) a
where a.rank_num <= 10;
"

## 加载数据
$hive -e "$sql"

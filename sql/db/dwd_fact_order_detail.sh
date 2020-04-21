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

insert overwrite table dwd_fact_order_detail partition (dt='$do_date')
select
    a.id           as id            , -- 订单编号
    a.order_id     as order_id      , -- 订单号
    b.user_id      as user_id       , -- 用户id
    a.sku_id       as sku_id        , -- sku商品id
    a.sku_name     as sku_name      , -- 商品名称
    a.order_price  as order_price   , -- 商品价格
    a.sku_num      as sku_num       , -- 商品数量
    a.create_time  as create_time   , -- 创建时间
    b.province_id  as province_id   , -- 省份ID
    a.order_price*a.sku_num as total_amount   -- 订单总金额
from
    (select * from ods_order_detail where dt='$do_date') a
inner join
    (select * from ods_order_info  where dt='$do_date') b
on a.order_id = b.id

"

## 加载数据
$hive -e "$sql"

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

insert overwrite table dwd_fact_cart_info partition (dt='$do_date')
select
    id            , -- 编号
    user_id       , -- 用户id
    sku_id        , -- skuid
    cart_price    , -- 放入购物车时价格
    sku_num       , -- 数量
    sku_name      , -- sku 名称 (冗余)
    create_time   , -- 创建时间
    operate_time  , -- 修改时间
    is_ordered    , -- 是否已经下单。1 为已下单,0 为未下单
    order_time     -- 下单时间
from ods_cart_info
where dt='$do_date'
"

## 加载数据
$hive -e "$sql"

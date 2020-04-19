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

insert overwrite table dwd_fact_comment_info partition (dt='$do_date')
select
    id            , -- 编号
    user_id       , -- 用户ID
    sku_id        , -- 商品sku
    spu_id        , -- 商品spu
    order_id      , -- 订单ID
    appraise      , -- 评价
    create_time     -- 评价时间
from ods_comment_info
where dt='$do_date';

"

## 加载数据
$hive -e "$sql"

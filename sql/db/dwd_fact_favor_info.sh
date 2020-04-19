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

insert overwrite table dwd_fact_favor_info partition (dt='$do_date')
select
    id            , -- 编
    user_id       , -- 用户id
    sku_id        , -- skuid
    spu_id        , -- spuid
    is_cancel     , -- 是否取消
    create_time   , -- 收藏时间
    cancel_time    -- 取消时间
from ods_favor_info
where dt = '$do_date';

"

## 加载数据
$hive -e "$sql"

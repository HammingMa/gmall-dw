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

insert into table ads_order_daycount
select '$do_date'                     as dt,           -- 统计日期
       sum(order_count)               as order_count,  -- 单日下单笔数
       sum(order_amount)              as order_amount, -- 单日下单金额
       sum(if(order_count > 0, 1, 0)) as order_users   -- 单日下单用户数
from dws_user_action_daycount
where dt = '$do_date';
"

## 加载数据
$hive -e "$sql"

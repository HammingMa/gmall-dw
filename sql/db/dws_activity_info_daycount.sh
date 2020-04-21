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
tmp_cnt as (
    select activity_id,
           sum(if(date_format(create_time,'yyyy-MM-dd') = '$do_date', 1, 0))  as order_count,
           sum(if(date_format(payment_time,'yyyy-MM-dd') = '$do_date', 1, 0)) as payment_count
    from dwd_fact_order_info
    where (dt = '$do_date' or dt = date_add('$do_date', -1))
      and activity_id is not null
    group by activity_id
)
insert overwrite table dws_activity_info_daycount partition (dt='$do_date')
select b.id,            -- 编号
       b.activity_name, -- 活动名称
       b.activity_type, -- 活动类型
       b.start_time,    -- 开始时间
       b.end_time,      -- 结束时间
       b.create_time,   -- 创建时间
       a.order_count,   -- 下单次数
       a.payment_count  -- 支付次数
from tmp_cnt a
         left join (select * from dwd_dim_activity_info where dt = '$do_date') b
                   on a.activity_id = b.id;
"

## 加载数据
$hive -e "$sql"

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

insert overwrite table dwd_dim_activity_info partition (dt='$do_date')
select
    a.id                , -- 编号
    a.activity_name     , -- 活动名称
    a.activity_type     , -- 活动类型
    b.condition_amount  , -- 满减金额
    b.condition_num     , -- 满减件数
    b.benefit_amount    , -- 优惠金额
    b.benefit_discount  , -- 优惠折扣
    b.benefit_level     , -- 优惠级别
    a.start_time        , -- 开始时间
    a.end_time          , -- 结束时间
    a.create_time         -- 创建时间
from
    (select * from ods_activity_info where dt='$do_date') a
        inner join
    (select * from ods_activity_rule where dt='$do_date') b
    on a.id=b.activity_id;

"

## 加载数据
$hive -e "$sql"

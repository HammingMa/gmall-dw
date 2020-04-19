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

insert into table ads_wastage_count
select '$do_date' as dt,
       count(*)      wastage_count
from (select mid_id
      from dws_uv_detail_day
      group by mid_id
      having max(dt) < date_add('$do_date', -7)
     ) a;
"

## 加载数据
$hive -e "$sql"

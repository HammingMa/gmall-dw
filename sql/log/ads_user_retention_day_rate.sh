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
insert into table ads_user_retention_day_rate
select
       '$do_date'                          as stat_date,
       a.create_date                       as create_date,
       a.retention_day                     as retention_day,
       a.retention_count                   as retention_count,
       b.new_mid_count                     as new_mid_count,
       a.retention_count / b.new_mid_count*100 as retention_ratio
from (select * from ads_user_retention_day_count where date_add(create_date,retention_day)='$do_date') a
         inner join ads_new_mid_count b
                    on a.create_date = b.create_date ;

"

## 加载数据
$hive -e "$sql"

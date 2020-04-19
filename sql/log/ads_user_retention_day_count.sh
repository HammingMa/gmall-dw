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
drop table if exists tmp_ads_user_retention_day_count_01;
create table tmp_ads_user_retention_day_count_01 as
select
       create_date,
       retention_day,
       count(*) as retention_count
from dws_user_retention_day
where dt = '$do_date'
group by create_date, retention_day;

insert overwrite table ads_user_retention_day_count
select
    *
from (
        select * from tmp_ads_user_retention_day_count_01
        union all
        select a.* from ads_user_retention_day_count a
            left join tmp_ads_user_retention_day_count_01 b
        on a.create_date =b.create_date and a.retention_day = b.retention_day
        where b.create_date is null
         ) a;

"

## 加载数据
$hive -e "$sql"

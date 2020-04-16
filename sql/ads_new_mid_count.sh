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
drop table if exists tmp_ads_new_mid_count_01;
create table tmp_ads_new_mid_count_01 as
select
    create_date,
    count(*) as new_mid_count
from dws_new_mid_day
where mn=date_format('$do_date','yyyy-MM') and create_date='$do_date'
group by create_date;

insert overwrite table ads_new_mid_count
select
    *
from (select * from ads_new_mid_count where create_date<>'$do_date'
        union all
      select * from tmp_ads_new_mid_count_01
    )a;


"

## 加载数据
$hive -e "$sql"

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
drop table if exists tmp_ads_uv_count_01;
create table tmp_ads_uv_count_01 as
select
    d.dt as dt,
    d.day_count as day_count,
    w.wk_count as wk_count,
    m.mn_count as mn_count,
    if('$do_date'=date_add(next_day('$do_date','MO'),-1),'Y','N') as is_weekend,
    if('$do_date'=last_day('$do_date'),'Y','N') as is_monthend
from
    (select
        '$do_date' as dt,
        count(*) as day_count
    from dws_uv_detail_day
    where dt='$do_date') d
    inner join
    (select
        '$do_date' as dt,
        count(*) as wk_count
    from dws_uv_detail_wk
    where wk_dt= concat(date_add(next_day('$do_date','MO'),-7),'_',date_add(next_day('$do_date','MO'),-1))) w
    on d.dt=w.dt
    inner join
    (select
        '$do_date' as dt,
        count(*) as mn_count
    from dws_uv_detail_mn
    where mn=date_format('$do_date','yyyy-MM')) m
    on d.dt= m.dt;


insert overwrite  table ads_uv_count partition(mn)
select
       *
from (select *,date_format('$do_date','yyyy-MM') as mn from tmp_ads_uv_count_01
    union all
    select * from ads_uv_count where mn=date_format('$do_date','yyyy-MM') and dt<>'$do_date') a;


"

## 加载数据
$hive -e "$sql"

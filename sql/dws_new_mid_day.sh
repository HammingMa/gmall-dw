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
drop table if exists tmp_dws_new_mid_day_01;
create table tmp_dws_new_mid_day_01 as
select
    a.mid_id,
    a.user_id ,
    a.version_code ,
    a.version_name ,
    a.lang ,
    a.source,
    a.os,
    a.area,
    a.model,
    a.brand,
    a.sdk_version,
    a.gmail,
    a.height_width,
    a.app_time,
    a.network,
    a.lng,
    a.lat,
    '$do_date' as create_date
from (select * from dws_uv_detail_day where dt='$do_date') a
left join dws_new_mid_day b
    on a.mid_id= b.mid_id
where b.mid_id is null;

insert overwrite table dws_new_mid_day partition (mn)
select
    *
from (select *,date_format('$do_date','yyyy-MM') as mn from tmp_dws_new_mid_day_01
union all
select * from dws_new_mid_day where mn=date_format('$do_date','yyyy-MM') and create_date<>'$do_date')a;


"

## 加载数据
$hive -e "$sql"

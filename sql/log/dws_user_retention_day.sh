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
insert overwrite table dws_user_retention_day partition (dt='$do_date')
select a.mid_id,
       a.user_id,
       a.version_code,
       a.version_name,
       a.lang,
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
       b.create_date                     as dt,
       datediff('$do_date', create_date) as retention_day
from (select * from dws_uv_detail_day where dt='$do_date') a
inner join  (select * from dws_new_mid_day
                where (mn = date_format(date_add('$do_date', -3), 'yyyy-MM')
                    or mn = date_format(date_add('$do_date', -1), 'yyyy-MM'))
                    and create_date >= date_add('$do_date', -3)
                    and create_date <= date_add('$do_date', -1)
            ) b
on a.mid_id = b.mid_id;;


"

## 加载数据
$hive -e "$sql"

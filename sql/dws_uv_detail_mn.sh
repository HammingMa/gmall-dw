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
insert overwrite table dws_uv_detail_mn partition (mn)
select
    mid_id as mid_id,
    concat_ws('|', collect_set(user_id)) as user_id,
    concat_ws('|', collect_set(version_code)) as version_code,
    concat_ws('|', collect_set(version_name)) as version_name,
    concat_ws('|', collect_set(lang))lang,
    concat_ws('|', collect_set(source)) as source,
    concat_ws('|', collect_set(os)) as os,
    concat_ws('|', collect_set(area)) as area,
    concat_ws('|', collect_set(model)) as model,
    concat_ws('|', collect_set(brand)) as brand,
    concat_ws('|', collect_set(sdk_version)) as sdk_version,
    concat_ws('|', collect_set(gmail)) as gmail,
    concat_ws('|', collect_set(height_width)) as height_width,
    concat_ws('|', collect_set(app_time)) as app_time,
    concat_ws('|', collect_set(network)) as network,
    concat_ws('|', collect_set(lng)) as lng,
    concat_ws('|', collect_set(lat)) as lat,
    date_format( '$do_date','yyyy-MM') as mn
from dws_uv_detail_day
where date_format(dt,'yyyy-MM') = date_format('$do_date','yyyy-MM')
group by mid_id;

"

## 加载数据
$hive -e "$sql"

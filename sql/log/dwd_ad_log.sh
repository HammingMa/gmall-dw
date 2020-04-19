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
use $db;
insert overwrite table dwd_ad_log PARTITION (dt='$do_date')
select
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.entry') entry,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.content') content,
    get_json_object(event_json,'$.kv.detail') detail,
    get_json_object(event_json,'$.kv.source') ad_source,
    get_json_object(event_json,'$.kv.behavior') behavior,
    get_json_object(event_json,'$.kv.newstype') newstype,
    get_json_object(event_json,'$.kv.show_style') show_style,
    server_time
from dwd_base_event_log
where dt='$do_date' and event_name='ad';

"

## 加载数据
$hive -e "$sql"

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
insert overwrite  table dwd_display_log partition (dt='$do_date')
select
    mid_id,
    user_id ,
    version_code,
    version_name ,
    lang,
    source,
    os,
    area ,
    model ,
    brand ,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network ,
    lng ,
    lat ,
    get_json_object(event_json,'$.kv.action') as action,
    get_json_object(event_json,'$.kv.action') as goodsid,
    get_json_object(event_json,'$.kv.action') as place,
    get_json_object(event_json,'$.kv.action') as extend1,
    get_json_object(event_json,'$.kv.action') as category,
    server_time
from dwd_base_event_log where dt='$do_date' and event_name='display';
"

## 加载数据
$hive -e "$sql"

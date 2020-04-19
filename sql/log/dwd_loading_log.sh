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
insert overwrite table dwd_loading_log PARTITION (dt='$do_date')
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
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.loading_time') loading_time,
    get_json_object(event_json,'$.kv.loading_way') loading_way,
    get_json_object(event_json,'$.kv.extend1') extend1,
    get_json_object(event_json,'$.kv.extend2') extend2,
    get_json_object(event_json,'$.kv.type') type,
    get_json_object(event_json,'$.kv.type1') type1,
    server_time
from dwd_base_event_log
where dt='$do_date' and event_name='loading';
"

## 加载数据
$hive -e "$sql"

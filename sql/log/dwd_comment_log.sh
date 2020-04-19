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
insert overwrite table dwd_comment_log PARTITION (dt='$do_date')
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
    get_json_object(event_json,'$.kv.comment_id') comment_id,
    get_json_object(event_json,'$.kv.userid') userid,
    get_json_object(event_json,'$.kv.p_comment_id') p_comment_id,
    get_json_object(event_json,'$.kv.content') content,
    get_json_object(event_json,'$.kv.addtime') addtime,
    get_json_object(event_json,'$.kv.other_id') other_id,
    get_json_object(event_json,'$.kv.praise_count') praise_count,
    get_json_object(event_json,'$.kv.reply_count') reply_count,
    server_time
from dwd_base_event_log
where dt='$do_date' and event_name='comment';

"

## 加载数据
$hive -e "$sql"

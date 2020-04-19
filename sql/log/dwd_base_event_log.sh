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
insert overwrite  table dwd_base_event_log partition (dt='$do_date')
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
    event_name,
    event_json,
    server_time
from (select
            get_json_object(event_log, '$.cm.mid') as mid_id,
            get_json_object(event_log, '$.cm.uid') as user_id ,
            get_json_object(event_log, '$.cm.vc') as version_code,
            get_json_object(event_log, '$.cm.vn') as version_name ,
            get_json_object(event_log, '$.cm.l') as lang ,
            get_json_object(event_log, '$.cm.sr') as source,
            get_json_object(event_log, '$.cm.os') as os,
            get_json_object(event_log, '$.cm.ar') as area ,
            get_json_object(event_log, '$.cm.md') as model ,
            get_json_object(event_log, '$.cm.ba') as brand ,
            get_json_object(event_log, '$.cm.sv') as sdk_version,
            get_json_object(event_log, '$.cm.g') as gmail ,
            get_json_object(event_log, '$.cm.hw') as height_width,
            get_json_object(event_log, '$.cm.t') as  app_time,
            get_json_object(event_log, '$.cm.nw') as network ,
            get_json_object(event_log, '$.cm.ln') as lng ,
            get_json_object(event_log, '$.cm.la') as lat ,
            get_json_object(event_log, '$.et')  as et,
            server_time
     from (select fields[0] as server_time,
                  fields[1] as event_log
           from (select split(line, '\\\|') as fields from ods_event_log where dt = '$do_date') a
          ) b
    )c lateral view flat_analizer(et) tmp_flat as event_name,event_json ;
"

## 加载数据
$hive -e "$sql"

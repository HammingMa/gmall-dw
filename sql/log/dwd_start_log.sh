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
insert overwrite table dwd_start_log partition (dt='${do_date}')
select
    get_json_object(line,'$.mid') mid_id                   ,
    get_json_object(line,'$.uid') user_id                  ,
    get_json_object(line,'$.vc') version_code             ,
    get_json_object(line,'$.vn') version_name             ,
    get_json_object(line,'$.l') lang                     ,
    get_json_object(line,'$.sr') source                   ,
    get_json_object(line,'$.os') os                       ,
    get_json_object(line,'$.ar') area                     ,
    get_json_object(line,'$.md') model                    ,
    get_json_object(line,'$.ba') brand                    ,
    get_json_object(line,'$.sv') sdk_version              ,
    get_json_object(line,'$.g') gmail                    ,
    get_json_object(line,'$.hw') height_width             ,
    get_json_object(line,'$.t') app_time                 ,
    get_json_object(line,'$.nw') network                  ,
    get_json_object(line,'$.ln') lng                      ,
    get_json_object(line,'$.la') lat                      ,
    get_json_object(line,'$.entry') entry                    ,
    get_json_object(line,'$.open_ad_type') open_ad_type             ,
    get_json_object(line,'$.action') action                   ,
    get_json_object(line,'$.loading_time') loading_time             ,
    get_json_object(line,'$.detail') detail                   ,
    get_json_object(line,'$.extend1') extend1
from ods_start_log
where dt='${do_date}';
"

## 加载数据
$hive -e "$sql"

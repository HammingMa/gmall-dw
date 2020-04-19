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

insert overwrite table dwd_dim_base_province
select
    a.id            as id                , -- id
    a.name          as province_name     , -- 省市名称
    a.area_code     as area_code         , -- 地区编码
    a.iso_code      as iso_code          , -- ISO 编码
    a.region_id     as region_id         , -- 地区id
    b.region_name   as region_name         -- 地区名称
from ods_base_province  a
    inner join ods_base_region b
    on  a.region_id =b.id;

"

## 加载数据
$hive -e "$sql"

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

insert into table ads_product_info
select '$do_date'   as dt,
       sum(sku_num) as sku_num,
       count(*)     as spu_num
from (
         select spu_id,
                count(*) as sku_num
         from dwt_sku_topic
         group by spu_id
     ) a;
"

## 加载数据
$hive -e "$sql"

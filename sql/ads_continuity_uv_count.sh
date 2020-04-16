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

insert into table ads_continuity_uv_count
select '$do_date'                                        as dt,
       concat(date_add('$do_date', -6), '_', '$do_date') as wk_dt,
       count(*)                                          as continuity_count
from (
         select mid_id
         from (
                  select mid_id,
                         diff_date
                  from (
                           select mid_id,
                                  date_sub(dt, rn) as diff_date
                           from (
                                    select mid_id,
                                           dt,
                                           row_number() over (partition by mid_id order by dt asc) as rn
                                    from dws_uv_detail_day
                                    where dt >= date_add('$do_date', -6)
                                      and dt <= '$do_date'
                                ) a
                       ) b
                  group by mid_id, diff_date
                  having count(*) >= 3
              ) c
         group by mid_id
     ) d;
"

## 加载数据
$hive -e "$sql"

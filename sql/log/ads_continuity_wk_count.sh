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

insert into table ads_continuity_wk_count
select '$do_date'                                                                                       as dt,
       concat(date_add(next_day('$do_date', 'MO'), -21), '_', date_add(next_day('$do_date', 'MO'), -1)) as wk_dt,
       count(*)                                                                                         as continuity_count
from (select mid_id
      from dws_uv_detail_wk
      where wk_dt >= concat(date_add(next_day('$do_date', 'MO'), -21), '_', date_add(next_day('$do_date', 'MO'), -15))
        and wk_dt <= concat(date_add(next_day('$do_date', 'MO'), -7), '_', date_add(next_day('$do_date', 'MO'), -1))
      group by mid_id
      having count(*) = 3
     ) a;
"

## 加载数据
$hive -e "$sql"

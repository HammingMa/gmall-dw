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

insert into ads_back_count
select
    '$do_date' as dt,
    concat(date_add(next_day('$do_date', 'MO'), -7), '_', date_add(next_day('$do_date', 'MO'), -1)) as wk_dt,
    count(*) as wastage_count
from
    (select mid_id
     from dws_uv_detail_wk
     where wk_dt = concat(date_add(next_day('$do_date', 'MO'), -7), '_', date_add(next_day('$do_date', 'MO'), -1))
    ) a
left join
    (select
        mid_id
    from dws_uv_detail_wk
    where wk_dt= concat( date_add(next_day('$do_date','MO'),-14),'_',date_add(next_day('$do_date','MO'),-8))
    ) b
on a.mid_id = b.mid_id
left join
    (select
        mid_id
    from dws_new_mid_day
    where  (mn = date_format(date_add(next_day('$do_date','MO'),-7), 'yyyy-MM')
        or mn = date_format(date_add(next_day('$do_date','MO'),-1), 'yyyy-MM'))
      and create_date >= date_add(next_day('$do_date','MO'),-7)
      and create_date <= date_add(next_day('$do_date','MO'),-1)
    ) c
    on a.mid_id = c.mid_id
where b.mid_id is null and c.mid_id is null;
"

## 加载数据
$hive -e "$sql"

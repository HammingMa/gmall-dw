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

insert into table ads_product_favor_topN
select *
from (select '$do_date'                                     as dt,          -- 统计日期
             sku_id                                         as sku_id,      -- 商品 ID
             favor_count                                    as favor_count, -- 收藏量
             row_number() over (order by favor_count desc ) as rank_num     -- 排名
      from dws_sku_action_daycount
      where dt = '$do_date') a
where a.rank_num <= 10;
"

## 加载数据
$hive -e "$sql"

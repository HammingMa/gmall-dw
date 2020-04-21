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

insert into table ads_product_cart_topN
select *
from (select '$do_date'                                  as dt,       -- 统计日期
             sku_id                                      as sku_id,   -- 商品 ID
             cart_num                                    as cart_num, -- 加入购物车数量
             row_number() over (order by cart_num desc ) as rank_num  -- 排名
      from dws_sku_action_daycount
      where dt = '$do_date') a
where a.rank_num <= 10;
"

## 加载数据
$hive -e "$sql"

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

insert into table ads_product_sale_topN
select *
from (select '$do_date'                                        as dt,             -- 统计日期
             sku_id                                            as sku_id,         -- 商品 ID
             payment_amount                                    as payment_amount, -- 销量
             row_number() over (order by payment_amount desc ) as rank_num        -- 排名
      from dws_sku_action_daycount
      where dt = '$do_date') a
where a.rank_num <= 10;
"

## 加载数据
$hive -e "$sql"

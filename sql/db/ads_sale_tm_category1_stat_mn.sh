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

insert overwrite table ads_sale_tm_category1_stat_mn
select tm_id,-- 品牌 id',
       category1_id,-- 1级品类id ',
       category1_name,-- 1 级品类名称 ',
       buycount,-- 购买人数',
       buy_twice_last,-- 两次以上购买人数',
       buy_twice_last / buycount          as buy_twice_last_ratio,-- 单次复购率',
       buy_3times_last,-- 三次以上购买人数',
       buy_3times_last / buycount         as buy_3times_last_ratio,-- 多次复购率',
       date_format('$do_date', 'yyyy-MM') as stat_mn, -- 统计月份
       '$do_date'                         as stat_date -- 统计日期
from (
         select sku_tm_id                      as tm_id,          -- 品牌 id
                sku_category1_id               as category1_id,   -- 1级品类id
                sku_category1_name             as category1_name, -- 1 级品类名称
                sum(if(order_count > 0, 1, 0)) as buycount,       -- 购买人数
                sum(if(order_count > 0, 1, 0)) as buy_twice_last, -- 两次以上购买人数
                sum(if(order_count > 0, 1, 0)) as buy_3times_last -- 三次以上购买人数
         from (
                  select user_id,
                         sku_tm_id,                      -- 品牌 id
                         sku_category1_id,               -- 1级品类id
                         sku_category1_name,             -- 1 级品类名称
                         sum(order_count) as order_count -- 购买次数
                  from dws_sale_detail_daycount
                  where date_format(dt, 'yyyy-MM') = date_format('$do_date', 'yyyy-MM')
                  group by user_id, sku_tm_id, sku_category1_id, sku_category1_name
              ) a
         group by sku_tm_id, sku_category1_id, sku_category1_name, date_format('$do_date', 'yyyy-MM'), '$do_date'
     ) b;
"

## 加载数据
$hive -e "$sql"

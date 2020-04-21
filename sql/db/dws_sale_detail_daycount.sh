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


with
tmp_order as (
    select user_id,
           sku_id,
           sum(sku_num)      as sku_num,
           count(*)          as order_count,
           sum(total_amount) as order_amount
    from dwd_fact_order_detail
    where dt = '$do_date'
    group by user_id, sku_id
),
tmp_user as (
    select *
    from dwd_dim_user_info_his
    where end_date = '9999-99-99'
),
tmp_sku as (
    select * from dwd_dim_sku_info where dt ='$do_date'
)
insert overwrite table dws_sale_detail_daycount partition (dt='$do_date')
select a.user_id,                                          -- 用户 id
       a.sku_id,                                           -- 商品 id
       b.gender,                                           -- 用户性别
       months_between('$do_date', b.birthday) / 12 as age, -- 用户年龄
       b.user_level,                                       -- 用户等级
       c.price,                                            -- 商品价格
       c.sku_name,                                         -- 商品名称
       c.tm_id,                                            -- 品牌id
       c.category3_id,                                     -- 商品三级品类 id
       c.category2_id,                                     -- 商品二级品类 id
       c.category1_id,                                     -- 商品一级品类 id
       c.category3_name,                                   -- 商品三级品类名称
       c.category2_name,                                   -- 商品二级品类名称
       c.category1_name,                                   -- 商品一级品类名称
       c.id,                                               -- 商品 spu
       a.sku_num,                                          -- 购买个数
       a.order_count,                                      -- 当日下单单数
       a.order_amount                                      -- 当日下单金额
from tmp_order a
         left join tmp_user b on a.user_id = b.id
         left join tmp_sku c on a.sku_id = c.id;

"

## 加载数据
$hive -e "$sql"

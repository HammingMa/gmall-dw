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

insert into table dwd_dim_sku_info partition (dt='$do_date')
select
    a.id              as id                , --商品id
    a.spu_id          as spu_id            , --spuid
    a.price           as price             , --商品价格
    a.sku_name        as sku_name          , --商品名称
    a.sku_desc        as sku_desc          , --商品描述
    a.weight          as weight            , --重量
    a.tm_id           as tm_id             , --品牌id
    b.tm_name         as tm_name           , --品牌名称
    c.category3_id    as category3_id      , --三级分类 id
    d.category2_id    as category2_id      , --二级分类 id
    e.category1_id    as category1_id      , --一级分类 id
    d.name            as category3_name    , --三级分类名称
    e.name            as category2_name    , --二级分类名称
    f.name            as category1_name    , --一级分类名称
    c.spu_name        as spu_name          , --spu 名称
    a.create_time     as create_time        --创建时间
from
    (
        select
            *
        from ods_sku_info
        where dt='$do_date'
    ) a
left join
    (
        select
            *
        from ods_base_trademark
        where dt='$do_date'
    ) b
on a.tm_id=b.tm_id
left join
    (
        select
            *
        from ods_spu_info
        where dt='$do_date'
    ) c
on a.spu_id = c.id
left join
    (
        select
            *
        from ods_base_category3
        where dt='$do_date'
    ) d
on a.category3_id=d.id
left join
    (
        select
            *
        from ods_base_category2
        where dt='$do_date'
    ) e
    on d.category2_id=e.id
left join
    (
        select
            *
        from ods_base_category2
        where dt='$do_date'
    ) f
    on e.category1_id=f.id;
"

## 加载数据
$hive -e "$sql"

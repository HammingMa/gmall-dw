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

drop table if exists tmp_dwd_fact_order_info_status;
create table tmp_dwd_fact_order_info_status as
select
    order_id,
    status_map['1001'] as create_time               , -- 创建时间(未支付状态)
    status_map['1002'] as payment_time              , -- 支付时间(已支付状态)
    status_map['1003'] as cancel_time               , -- 取消时间(已取消状态)
    status_map['1004'] as finish_time               , -- 完成时间(已完成状态)
    status_map['1005'] as refund_time               , -- 退款时间(退款中状态)
    status_map['1006'] as refund_finish_time          -- 退款完成时间(退款完成状态
from
    (
        select
            order_id,
            str_to_map(status,',',':') as status_map
        from
            (
                select
                    order_id,
                    concat_ws(',',collect_set(concat(order_status,':',operate_time))) as status
                from ods_order_status_log
                where dt='$do_date'
                group by order_id
            ) a
    )b;



drop table if exists tmp_dwd_fact_order_info_new;
create table tmp_dwd_fact_order_info_new as
select
    a.id                        , -- 订单编号
    a.order_status              , -- 订单状态
    a.user_id                   , -- 用户id
    a.out_trade_no              , -- 支付流水号
    b.create_time               , -- 创建时间(未支付状态)
    b.payment_time              , -- 支付时间(已支付状态)
    b.cancel_time               , -- 取消时间(已取消状态)
    b.finish_time               , -- 完成时间(已完成状态)
    b.refund_time               , -- 退款时间(退款中状态)
    b.refund_finish_time        , -- 退款完成时间(退款完成状态
    a.province_id               , -- 省份ID
    c.activity_id               , -- 活动ID
    a.original_total_amount     , -- 原价金额
    a.benefit_reduce_amount     , -- 优惠金额
    a.feight_fee                , -- 运费
    a.final_total_amount        ,  -- 订单金额
    date_format(b.create_time,'yyyy-MM-dd') as dt -- 分区时间
from
     (select * from ods_order_info where dt='$do_date' ) a
inner join
         tmp_dwd_fact_order_info_status b
    on a.id=b.order_id
left join
    (select * from ods_activity_order where dt='$do_date') c
    on a.id=c.order_id
;
insert overwrite table dwd_fact_order_info partition (dt)
select
    b.id                        , -- 订单编号
    b.order_status              , -- 订单状态
    b.user_id                   , -- 用户id
    b.out_trade_no              , -- 支付流水号
    b.create_time               , -- 创建时间(未支付状态)
    b.payment_time              , -- 支付时间(已支付状态)
    b.cancel_time               , -- 取消时间(已取消状态)
    b.finish_time               , -- 完成时间(已完成状态)
    b.refund_time               , -- 退款时间(退款中状态)
    b.refund_finish_time        , -- 退款完成时间(退款完成状态
    b.province_id               , -- 省份ID
    b.activity_id               , -- 活动ID
    b.original_total_amount     , -- 原价金额
    b.benefit_reduce_amount     , -- 优惠金额
    b.feight_fee                , -- 运费
    b.final_total_amount        ,  -- 订单金额
    b.dt -- 分区时间
from
    (
        select
            *,
            row_number() over (partition by id  order by update_time desc) as rn
        from
        (   select *,
                   coalesce(refund_finish_time,refund_time,finish_time,cancel_time,payment_time,create_time) as update_time
                from dwd_fact_order_info where dt in (select dt from tmp_dwd_fact_order_info_new group by dt)
        union  all
            select *,
                   coalesce(refund_finish_time,refund_time,finish_time,cancel_time,payment_time,create_time) as update_time
            from tmp_dwd_fact_order_info_new
            )  a
    ) b
where b.rn=1;

"

## 加载数据
$hive -e "$sql"

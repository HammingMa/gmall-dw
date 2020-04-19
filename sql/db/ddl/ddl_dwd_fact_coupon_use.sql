use gmall;

drop table if exists dwd_fact_coupon_use;
create external table dwd_fact_coupon_use(
    `id`            string COMMENT '编号',
    `coupon_id`     string COMMENT '优惠券 ID',
    `user_id`       string COMMENT 'userid',
    `order_id`      string COMMENT '订单id',
    `coupon_status` string COMMENT '优惠券状态',
    `get_time`      string COMMENT '领取时间',
    `using_time`    string COMMENT '使用时间(下单)',
    `used_time`     string COMMENT '使用时间(支付)'
) COMMENT '优惠券领用事实表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/dwd/dwd_fact_coupon_use/';


insert overwrite table dwd_fact_coupon_use partition (dt)
select
    coalesce(new.id           ,old.id           ) as id            , -- 编号
    coalesce(new.coupon_id    ,old.coupon_id    ) as coupon_id     , -- 优惠券 ID
    coalesce(new.user_id      ,old.user_id      ) as user_id       , -- userid
    coalesce(new.order_id     ,old.order_id     ) as order_id      , -- 订单id
    coalesce(new.coupon_status,old.coupon_status) as coupon_status , -- 优惠券状态
    coalesce(new.get_time     ,old.get_time     ) as get_time      , -- 领取时间
    coalesce(new.using_time   ,old.using_time   ) as using_time    , -- 使用时间(下单)
    coalesce(new.used_time    ,old.used_time    ) as used_time     , -- 使用时间(支付)
    coalesce(date_format(new.get_time,'yyy-MM-dd'),date_format(old.get_time,'yyy-MM-dd')) as dt
from
    (
    select * from dwd_fact_coupon_use
    where dt in (select date_format(get_time,'yyy-MM-dd')
                    from ods_coupon_use where dt='$do_date')
    ) old
full join
    (
        select * from ods_coupon_use where dt='$do_date'
    ) new;
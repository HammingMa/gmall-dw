use gmall;

drop table if exists dws_activity_info_daycount;
create external table dws_activity_info_daycount
(
    `id`            string COMMENT '编号',
    `activity_name` string COMMENT '活动名称',
    `activity_type` string COMMENT '活动类型',
    `start_time`    string COMMENT '开始时间',
    `end_time`      string COMMENT '结束时间',
    `create_time`   string COMMENT '创建时间',
    `order_count`   bigint COMMENT '下单次数',
    `payment_count` bigint COMMENT '支付次数'
) COMMENT '购物车信息表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/dws/dws_activity_info_daycount/' tblproperties ("parquet.compression" = "lzo");

with
tmp_cnt as (
    select activity_id,
           sum(if(date_format(create_time,'yyyy-MM-dd') = '$do_date', 1, 0))  as order_count,
           sum(if(date_format(payment_time,'yyyy-MM-dd') = '$do_date', 1, 0)) as payment_count
    from dwd_fact_order_info
    where (dt = '$do_date' or dt = date_add('$do_date', -1))
      and activity_id is not null
    group by activity_id
)
insert overwrite table dws_activity_info_daycount partition (dt='$do_date')
select b.id,            -- 编号
       b.activity_name, -- 活动名称
       b.activity_type, -- 活动类型
       b.start_time,    -- 开始时间
       b.end_time,      -- 结束时间
       b.create_time,   -- 创建时间
       a.order_count,   -- 下单次数
       a.payment_count  -- 支付次数
from tmp_cnt a
         left join (select * from dwd_dim_activity_info where dt = '$do_date') b
                   on a.activity_id = b.id

select * from dws_activity_info_daycount
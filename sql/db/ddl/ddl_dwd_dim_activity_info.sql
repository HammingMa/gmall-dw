use gmall;

drop table if exists dwd_dim_activity_info;
create external table dwd_dim_activity_info(
    `id`                string COMMENT '编号',
    `activity_name`     string COMMENT '活动名称',
    `activity_type`     string COMMENT '活动类型',
    `condition_amount`  string COMMENT '满减金额',
    `condition_num`     string COMMENT '满减件数',
    `benefit_amount`    string COMMENT '优惠金额',
    `benefit_discount`  string COMMENT '优惠折扣',
    `benefit_level`     string COMMENT '优惠级别',
    `start_time`        string COMMENT '开始时间',
    `end_time`          string COMMENT '结束时间',
    `create_time`       string COMMENT '创建时间'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t'
    stored as parquet
    location '/warehouse/gmall/dwd/dwd_dim_activity_info/'
    tblproperties ("parquet.compression"="lzo");


insert overwrite table dwd_dim_activity_info partition (dt='$do_date')
select
    a.id                , -- 编号
    a.activity_name     , -- 活动名称
    a.activity_type     , -- 活动类型
    b.condition_amount  , -- 满减金额
    b.condition_num     , -- 满减件数
    b.benefit_amount    , -- 优惠金额
    b.benefit_discount  , -- 优惠折扣
    b.benefit_level     , -- 优惠级别
    a.start_time        , -- 开始时间
    a.end_time          , -- 结束时间
    a.create_time         -- 创建时间
from
    (select * from ods_activity_info where dt='$do_date') a
        inner join
    (select * from ods_activity_rule where dt='$do_date') b
    on a.id=b.activity_id;
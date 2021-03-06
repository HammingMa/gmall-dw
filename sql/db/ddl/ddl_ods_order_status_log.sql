use gmall;

drop table if exists ods_order_status_log;
create external table ods_order_status_log (
    `id` bigint COMMENT'编号',
    `order_id` string COMMENT '订单 ID',
    `order_status` string COMMENT '订单状态',
    `operate_time` string COMMENT '修改时间'
) COMMENT '订单状态表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t' STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location '/warehouse/gmall/ods/ods_order_status_log/';

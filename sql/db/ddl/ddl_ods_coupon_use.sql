use gmall;

drop table if exists ods_coupon_use;
create external table ods_coupon_use(
    `id` string COMMENT '编号',
    `coupon_id` string COMMENT '优惠券 ID',
    `user_id` string COMMENT 'skuid',
    `order_id` string COMMENT 'spuid',
    `coupon_status` string COMMENT '优惠券状态',
    `get_time` string COMMENT '领取时间',
    `using_time` string COMMENT '使用时间(下单)',
    `used_time` string COMMENT '使用时间(支付)',
    `expire_time` string COMMENT '过期时间'
) COMMENT '优惠券领用表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t' STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location '/warehouse/gmall/ods/ods_coupon_use/';
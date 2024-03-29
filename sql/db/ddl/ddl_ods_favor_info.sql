use gmall;

drop table if exists ods_favor_info;
create external table ods_favor_info(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户 id',
    `sku_id` string COMMENT 'skuid',
    `spu_id` string COMMENT 'spuid',
    `is_cancel` string COMMENT '是否取消',
    `create_time` string COMMENT '收藏时间',
    `cancel_time` string COMMENT '取消时间'
) COMMENT '商品收藏表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t' STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location '/warehouse/gmall/ods/ods_favor_info/';
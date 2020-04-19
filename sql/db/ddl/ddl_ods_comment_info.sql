use gmall;

drop table if exists ods_comment_info;
create external table ods_comment_info(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户 ID',
    `sku_id` string COMMENT '商品 sku',
    `spu_id` string COMMENT '商品 spu',
    `order_id` string COMMENT '订单 ID',
    `appraise` string COMMENT '评价',
    `comment_txt` String comment '评论详情',
    `create_time` string COMMENT '评价时间',
    `operate_time` string COMMENT ''
) COMMENT '商品评论表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t' STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location '/warehouse/gmall/ods/ods_comment_info/';
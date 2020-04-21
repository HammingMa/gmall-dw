use gmall;

drop table if exists ods_order_detail;
create external table ods_order_detail(
    `id` string COMMENT '订单编号',
    `order_id` string COMMENT '订单号',
    `sku_id` string COMMENT '商品 id',
    `sku_name` string COMMENT '商品名称',
    `img_url` string COMMENT '图片地址',
    `order_price` decimal(10,2) COMMENT '商品价格',
    `sku_num` bigint COMMENT '商品数量',
    `create_time` string COMMENT '创建时间'
) COMMENT '订单详情表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t' STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_order_detail/';
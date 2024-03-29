use gmall;

drop table if exists ods_sku_info;
create external table ods_sku_info(
    `id` string COMMENT 'skuId',
    `spu_id`string COMMENT'spuid',
    `price` decimal(10,2) COMMENT '价格',
    `sku_name` string COMMENT '商品名称',
    `sku_desc` string COMMENT '商品描述',
    `weight` string COMMENT '重量',
    `tm_id` string COMMENT '品牌 id',
    `category3_id` string COMMENT '品类 id',
    `sku_default_img` string COMMENT '默认sku图片',
    `create_time` string COMMENT '创建时间'
) COMMENT 'SKU 商品表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t' STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_sku_info/';



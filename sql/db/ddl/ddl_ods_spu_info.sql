use gmall;

drop table if exists ods_spu_info;
create external table ods_spu_info(
    `id` string COMMENT 'spuid',
    `description` string COMMENT '描述',
    `spu_name` string COMMENT 'spu 名称',
    `category3_id` string COMMENT '品类 id',
    `tm_id` string COMMENT '品牌 id'
) COMMENT 'SPU 商品表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t' STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location '/warehouse/gmall/ods/ods_spu_info/';



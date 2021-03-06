use gmall;


drop table if exists ods_base_trademark;
create external table ods_base_trademark (
    `tm_id` bigint COMMENT'编号',
    `tm_name` string COMMENT '品牌名称'
) COMMENT '品牌表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t' STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location '/warehouse/gmall/ods/ods_base_trademark/';

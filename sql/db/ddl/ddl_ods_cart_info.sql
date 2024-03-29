use gmall;

drop table if exists ods_cart_info;
create external table ods_cart_info(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户 id',
    `sku_id` string COMMENT 'skuid',
    `cart_price` string COMMENT '放入购物车时价格',
    `sku_num` string COMMENT '数量',
    `img_url` string COMMENT '图片地址',
    `sku_name` string COMMENT 'sku 名称 (冗余)',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '修改时间',
    `is_ordered` string COMMENT '是否已经下单',
    `order_time` string COMMENT '下单时间'
) COMMENT '加购表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t' STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location '/warehouse/gmall/ods/ods_cart_info/';
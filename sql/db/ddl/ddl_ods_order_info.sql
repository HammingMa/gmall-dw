use gmall;

drop table if exists ods_order_info;

create external table ods_order_info (
    `id` string COMMENT '订单号',
    `consignee` string COMMENT '收货人',
    `consignee_tel` string COMMENT '收货人电话',
    `final_total_amount` decimal(10,2) COMMENT '订单金额',
    `order_status` string COMMENT '订单状态',
    `user_id` string COMMENT '用户 id',
    `delivery_address` string COMMENT '配送地址',
    `order_comment` string COMMENT '订单备注',
    `out_trade_no` string COMMENT '支付流水号',
    `trade_body` string COMMENT '订单描述(第三方支付用)',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `expire_time` string COMMENT '失效时间',
    `tracking_no` string COMMENT '物流单编号',
    `parent_order_id` string COMMENT '父订单编号',
    `img_url` string COMMENT '图片路径',
    `province_id` string COMMENT '省份 ID',
    `benefit_reduce_amount` decimal(10,2) COMMENT '优惠金额',
    `original_total_amount` decimal(10,2) COMMENT '原价金额',
    `feight_fee` decimal(10,2) COMMENT '运费'
) COMMENT '订单表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t' STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location '/warehouse/gmall/ods/ods_order_info/';



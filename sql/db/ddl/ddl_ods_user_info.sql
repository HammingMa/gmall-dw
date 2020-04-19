use gmall;

drop table if exists ods_user_info;
create external table ods_user_info(
    `id` string COMMENT '用户 id',
    `login_name` string COMMENT '登录名',
    `nick_name` string COMMENT '昵称',
    `passwd` string COMMENT '免密',
    `name` string COMMENT '姓名',
    `phone_num` string COMMENT '手机号码',
    `email` string COMMENT '邮箱',
    `head_img` string COMMENT '头像图片',
    `user_level` string COMMENT '用户等级',
    `birthday` string COMMENT '生日',
    `gender` string COMMENT '性别',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间'
) COMMENT '用户表'
    PARTITIONED BY (`dt` string)
    row format delimited fields terminated by '\t' STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    location '/warehouse/gmall/ods/ods_user_info/';
drop table if exists dwd_display_log;
CREATE EXTERNAL TABLE dwd_display_log(
    `mid_id` string,
    `user_id` string,
    `version_code` string,
    `version_name` string,
    `lang` string,
    `source` string,
    `os` string,
    `area` string,
    `model` string,
    `brand` string,
    `sdk_version` string,
    `gmail` string,
    `height_width` string,
    `app_time` string,
    `network` string,
    `lng` string,
    `lat` string,
    `action` string,
    `goodsid` string,
    `place` string,
    `extend1` string,
    `category` string,
    `server_time` string
)
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_display_log/'
TBLPROPERTIES('parquet.compression'='lzo');



insert overwrite  table dwd_display_log partition (dt='2020-04-02')
select
    mid_id,
    user_id ,
    version_code,
    version_name ,
    lang,
    source,
    os,
    area ,
    model ,
    brand ,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network ,
    lng ,
    lat ,
    get_json_object(event_json,"$.kv.action") as action,
    get_json_object(event_json,"$.kv.action") as goodsid,
    get_json_object(event_json,"$.kv.action") as place,
    get_json_object(event_json,"$.kv.action") as extend1,
    get_json_object(event_json,"$.kv.action") as category,
    server_time
from dwd_base_event_log where dt='2020-04-02' and event_name='display';
use gmall;

drop table if exists dwd_ad_log;
CREATE EXTERNAL TABLE dwd_ad_log(
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
    `entry` string,
    `action` string,
    `content` string,
    `detail` string,
    `ad_source` string,
    `behavior` string,
    `newstype` string,
    `show_style` string,
    `server_time` string
)
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_ad_log/'
TBLPROPERTIES('parquet.compression'='lzo');

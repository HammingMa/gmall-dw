use gmall;

drop table if exists ads_user_retention_day_rate;
create external table ads_user_retention_day_rate
(
    `stat_date`       string comment '统计日期',
    `create_date`     string comment '设备新增日期',
    `retention_day`   int comment '截止当前日期留存天数',
    `retention_count` bigint comment '留存数量',
    `new_mid_count`   bigint comment '当日设备新增数量',
    `retention_ratio` decimal(10, 2) comment '留存率'
) COMMENT '每日用户留存情况'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_user_retention_day_rate/';


insert into table ads_user_retention_day_rate
select
       '$do_date'                          as stat_date,
       a.create_date                       as create_date,
       a.retention_day                     as retention_day,
       a.retention_count                   as retention_count,
       b.new_mid_count                     as new_mid_count,
       a.retention_count / b.new_mid_count*100 as retention_ratio
from (select * from ads_user_retention_day_count where date_add(create_date,create_date)='$do_date') a
         inner join ads_new_mid_count b
                    on a.create_date = b.create_date ;


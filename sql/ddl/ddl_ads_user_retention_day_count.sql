use gmall;

drop table if exists ads_user_retention_day_count;
create external table ads_user_retention_day_count
(
    `create_date`       string  comment '设备新增日期',
    `retention_day`     int comment '截止当前日期留存天数',
    `retention_count`    bigint comment  '留存数量'
)  COMMENT '每日用户留存情况'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_retention_day_count/';






drop table if exists tmp_ads_user_retention_day_count_01;
create table tmp_ads_user_retention_day_count_01 as
select
       create_date,
       retention_day,
       count(*) as retention_count
from dws_user_retention_day
where dt = 'do_date'
group by create_date, retention_day;

insert overwrite table ads_user_retention_day_count
select
    *
from (
        select * from tmp_ads_user_retention_day_count_01
        union all
        select a.* from ads_user_retention_day_count a
            left join tmp_ads_user_retention_day_count_01 b
        on a.create_date =b.create_date and a.retention_day = b.retention_day
        where b.create_date is null
         ) a;
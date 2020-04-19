use gmall;

drop table if exists ads_new_mid_count;
create external table ads_new_mid_count
(
    `create_date`     string comment '创建时间' ,
    `new_mid_count`   BIGINT comment '新增设备数量'
)  COMMENT '每日新增设备信息数量'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_new_mid_count/';



drop table if exists tmp_ads_new_mid_count_01;
create table tmp_ads_new_mid_count_01 as
select
    create_date,
    count(*) as new_mid_count
from dws_new_mid_day
where mn=date_format('$do_date','yyyy-MM') and create_date='$do_date'
group by create_date;

insert overwrite table ads_new_mid_count
select
    *
from (select * from ads_new_mid_count where create_date<>'$do_date'
        union all
      select * from tmp_ads_new_mid_count_01
    )a;


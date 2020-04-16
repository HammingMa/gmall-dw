use gmall;

drop table if exists ads_silent_count;
create external table ads_silent_count(
    `dt` string COMMENT '统计日期',
    `silent_count` bigint COMMENT '沉默设备数'
)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_silent_count';

insert into table ads_silent_count
select
       '$do_date' as dt,
       count(*)   as silent_count
from (select mid_id
      from dws_uv_detail_day
      group by mid_id
      having count(*) = 1
         and min(dt) < date_add('$do_date', -7)
     ) a ;
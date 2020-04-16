use gmall;

drop table if exists ads_wastage_count;
create external table ads_wastage_count(
    `dt` string COMMENT '统计日期',
    `wastage_count` bigint COMMENT '流失设备数'
)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_wastage_count';


insert into table ads_wastage_count
select '$do_date' as dt,
       count(*)      wastage_count
from (select mid_id
      from dws_uv_detail_day
      group by mid_id
      having max(dt) < date_add('$do_date', -7)
     ) a
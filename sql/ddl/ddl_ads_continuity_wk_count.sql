use gmall;

drop table if exists ads_continuity_wk_count;
create external table ads_continuity_wk_count(
    `dt` string COMMENT '统计日期,一般用结束周周日日期,如果每天计算一次,可用当天日期',
    `wk_dt` string COMMENT '持续时间',
    `continuity_count` bigint
)comment '最近联系三周活跃的用户数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_wk_count';


insert into table ads_continuity_wk_count
select '$do_date'                                                                                       as dt,
       concat(date_add(next_day('$do_date', 'MO'), -21), '_', date_add(next_day('$do_date', 'MO'), -1)) as wk_dt,
       count(*)                                                                                         as continuity_count
from (select mid_id
      from dws_uv_detail_wk
      where wk_dt >= concat(date_add(next_day('$do_date', 'MO'), -21), '_', date_add(next_day('$do_date', 'MO'), -15))
        and wk_dt <= concat(date_add(next_day('$do_date', 'MO'), -7), '_', date_add(next_day('$do_date', 'MO'), -1))
      group by mid_id
      having count(*) = 3
     ) a;
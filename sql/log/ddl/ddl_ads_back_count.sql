use gmall;

drop table if exists ads_back_count;
create external table ads_back_count(
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '统计日期所在周',
    `wastage_count` bigint COMMENT '回流设备数'
)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_back_count';

insert into ads_back_count
select
    '$do_date' as dt,
    concat(date_add(next_day('$do_date', 'MO'), -7), '_', date_add(next_day('$do_date', 'MO'), -1)) as wk_dt,
    count(*) as wastage_count
from
    (select mid_id
     from dws_uv_detail_wk
     where wk_dt = concat(date_add(next_day('$do_date', 'MO'), -7), '_', date_add(next_day('$do_date', 'MO'), -1))
    ) a
left join
    (select
        mid_id
    from dws_uv_detail_wk
    where wk_dt= concat( date_add(next_day('$do_date','MO'),-14),'_',date_add(next_day('$do_date','MO'),-8))
    ) b
on a.mid_id = b.mid_id
left join
    (select
        min_id
    from dws_new_mid_day
    where  (mn = date_format(date_add(next_day('$do_date','MO'),-7), 'yyyy-MM')
        or mn = date_format(date_add(next_day('$do_date','MO'),-1), 'yyyy-MM'))
      and create_date >= date_add(next_day('$do_date','MO'),-7)
      and create_date <= date_add(next_day('$do_date','MO'),-1)
    ) c
    on a.mid_id = c.mid_id
where b.mid_id is null and c.mid_id is null;
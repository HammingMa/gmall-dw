use gmall;
drop table if exists ads_uv_count;
create external table ads_uv_count(
    `dt` string COMMENT '统计日期',
    `day_count` bigint COMMENT '当日用户数量',
    `wk_count`  bigint COMMENT '当周用户数量',
    `mn_count`  bigint COMMENT '当月用户数量',
    `is_weekend` string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
    `is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果'
) COMMENT '活跃设备数'
partitioned by (mn String comment '分区月')
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_uv_count/'
;

drop table if exists tmp_ads_uv_count_01;
create table tmp_ads_uv_count_01 as
select
    d.dt as dt,
    d.day_count as day_count,
    w.wk_count as wk_count,
    m.mn_count as mn_count,
    if('$do_date'=date_add(next_day('$do_date','MO'),-1),'Y','N') as is_weekend,
    if('$do_date'=last_day('$do_date'),'Y','N') as is_monthend
from
    (select
        '$do_date' as dt,
        count(*) as day_count
    from dws_uv_detail_day
    where dt='$do_date') d
    inner join
    (select
        '$do_date' as dt,
        count(*) as wk_count
    from dws_uv_detail_wk
    where wk_dt= concat(date_add(next_day('$do_date','MO'),-7),'_',date_add(next_day('$do_date','MO'),-1))) w
    on d.dt=w.dt
    inner join
    (select
        '$do_date' as dt,
        count(*) as mn_count
    from dws_uv_detail_mn
    where mn=date_format('$do_date','yyyy-MM')) m
    on d.dt= m.dt;


insert overwrite  table ads_uv_count partition(mn)
select
       *
from (select *,date_format('$do_date','yyyy-MM') as mn from tmp_ads_uv_count_01
    union all
    select * from ads_uv_count where mn=date_format('$do_date','yyyy-MM') and dt<>'$do_date') a;


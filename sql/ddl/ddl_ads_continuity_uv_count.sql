use gmall;


drop table if exists ads_continuity_uv_count;
create external table ads_continuity_uv_count
(
    `dt`               string COMMENT '统计日期',
    `wk_dt`            string COMMENT '最近7天日期',
    `continuity_count` bigint
) COMMENT '最近七天连续三天连续活跃设备数'
    row format delimited fields terminated by '\t'
    location '/warehouse/gmall/ads/ads_continuity_uv_count';


insert into table ads_continuity_uv_count
select '$do_date'                                        as dt,
       concat(date_add('$do_date', -6), '_', '$do_date') as wk_dt,
       count(*)                                          as continuity_count
from (
         select mid_id
         from (
                  select mid_id,
                         diff_date
                  from (
                           select mid_id,
                                  date_sub(dt, rn) as diff_date
                           from (
                                    select mid_id,
                                           dt,
                                           row_number() over (partition by mid_id order by dt asc) as rn
                                    from dws_uv_detail_day
                                    where dt >= date_add('$do_date', -6)
                                      and dt <= '$do_date'
                                ) a
                       ) b
                  group by mid_id, diff_date
                  having count(*) >= 3
              ) c
         group by mid_id
     ) d;




select
    count(*)
from
(
    select
        mid_id
    from
    (
        select mid_id,
               rn
        from (
                 select mid_id,
                        dt,
                        pre_dt,
                        sum(if(datediff(dt, pre_dt) = 1, 0, 1)) over (partition by mid_id order by dt asc) as rn
                 from (
                          select mid_id,
                                 dt,
                                 lag(dt, 1, '9999-01-01') over (partition by mid_id order by dt asc) as pre_dt
                          from dws_uv_detail_day
                          where dt >= date_add('2020-04-10', -6)
                            and dt <= '2020-04-10'
                      ) a
             ) b
        group by mid_id, rn
        having count(*) >= 3
    )c
    group by  mid_id
) d;

select
count(distinct mid_id)
from dws_uv_detail_day
where dt >= date_add('2020-04-10', -6)
  and dt <= '2020-04-10'

create table  temp_01 as
select mid_id
from (
         select mid_id,
                diff_date
         from (
                  select mid_id,
                         date_sub(dt, rn) as diff_date
                  from (
                           select mid_id,
                                  dt,
                                  row_number() over (partition by mid_id order by dt asc) as rn
                           from dws_uv_detail_day
                           where dt >= date_add('2020-04-10', -6)
                             and dt <= '2020-04-10'
                       ) a
              ) b
         group by mid_id, diff_date
         having count(*) >= 3
     ) c
group by mid_id;

create table  temp_02 as
select
    mid_id
from
    (
        select mid_id,
               rn
        from (
                 select mid_id,
                        dt,
                        next_dt,
                        sum(if(datediff(next_dt, dt) = 1, 0, 1)) over (partition by mid_id order by dt asc) as rn
                 from (
                          select mid_id,
                                 dt,
                                 lead(dt, 1, '9999-01-01') over (partition by mid_id order by dt asc) as next_dt
                          from dws_uv_detail_day
                          where dt >= date_add('2020-04-10', -6)
                            and dt <= '2020-04-10'
                      ) a
             ) b
        group by mid_id, rn
        having count(*) >= 3
    )c
group by  mid_id;

select a.mid_id
from temp_01 a
         left join temp_02 b on a.mid_id = b.mid_id
where b.mid_id is null;


select mid_id,dt from dws_uv_detail_day where mid_id='989' order by dt asc;


select mid_id,
       dt,
       pre_dt,
       sum(if(datediff(dt,pre_dt) = 1, 0, 1)) over (partition by mid_id order by dt asc) as rn
from (
         select mid_id,
                dt,
                lag(dt, 1, '9999-01-01') over (partition by mid_id order by dt asc) as pre_dt
         from dws_uv_detail_day
         where dt >= date_add('2020-04-10', -6)
           and dt <= '2020-04-10'
        and mid_id='989'
     ) a
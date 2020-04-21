#!/bin/bash

if [ -n $1 ]; then
    do_date=$1
else
    do_date=`date -d '-1 day' +%F`
fi

echo $do_date

db=gmall
hive=/opt/hive/bin/hive
hadoop=/opt/hadoop/bin/hadoop

sql="
set hive.exec.dynamic.partition.mode=nonstrict;
use $db;

insert into table ads_user_topic
select dt                    as dt,                    -- 统计日期
       day_users             as day_users,             -- 活跃会员数
       day_new_users         as day_new_users,         -- 新增会员数
       day_new_payment_users as day_new_payment_users, -- 新增消费会员数
       payment_users         as payment_users,         -- 总付费会员数
       users                 as users,                 -- 总会员数
       day_users / users     as day_users2users,       -- 会员活跃率
       payment_users / users as payment_users2users,   -- 会员付费率
       day_new_users / users as day_new_users2users    -- 会员新鲜度
from (
         select '$do_date'                                      as dt,                    -- 统计日期
                sum(if(login_date_last = '$do_date', 1, 0))     as day_users,             -- 活跃会员数
                sum(if(login_date_first = '$do_date', 1, 0))    as day_new_users,         -- 新增会员数
                sum(if(payment_date_first = '$do_date', 1, 0)) as day_new_payment_users, -- 新增消费会员数
                sum(if(payment_count > 0, 1, 0))                as payment_users,         -- 总付费会员数
                count(*)                                        as users                  -- 总会员数
         from dwt_user_topic
     ) a;

"

## 加载数据
$hive -e "$sql"

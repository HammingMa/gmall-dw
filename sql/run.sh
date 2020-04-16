#!/bin/bash


begin_date="2020-04-05"
end_date="2020-04-10"
i=1
while [ $i -le 6 ];
do
    echo $begin_date
    ## ./ods_start_log.sh $begin_date
    ## ./dwd_start_log.sh $begin_date
    ## ./dws_uv_detail_day.sh $begin_date
    ## ./dws_new_mid_day.sh $begin_date
    ## ./ads_new_mid_count.sh $begin_date
    ## ./dws_user_retention_day.sh $begin_date
    ./ads_user_retention_day_count.sh $begin_date
    ./ads_user_retention_day_rate.sh $begin_date

    begin_date=`date -d " $begin_date  1 day" +%F`
    let i+=1
done
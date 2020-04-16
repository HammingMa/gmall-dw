#!/bin/bash

if [ -n $1 ]; then
    do_date=$1
else
    do_date=`date -d '-1 day' +%F`
fi

echo $do_date

hive=/opt/hive/bin/hive
hadoop=/opt/hadoop/bin/hadoop

sql="load data inpath '/origin_data/gmall/log/topic_start/$do_date' into table gmall.ods_start_log partition (dt='$do_date');"

## 加载数据
$hive -e "$sql"

## 建立索引
$hadoop jar /opt/hadoop/share/hadoop/common/hadoop-lzo-0.4.20.jar \
com.hadoop.compression.lzo.DistributedLzoIndexer \
/warehouse/gmall/ods/ods_start_log/dt=$do_date
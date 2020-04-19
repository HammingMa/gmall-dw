#!/bin/bash

if [ -n $1 ]; then
    do_date=$1
else
    do_date=`date -d '-1 day' +%F`
fi

echo $do_date

jdbc=jdbc:mysql://hdp3:3306/gmall1.2
username=root
password=root
db_name=gmall
##表名
table_name=base_dic
##导入的列 导入全部列 直接用 * ,否则逗号分隔的类名
cloumns=*
##导入方式 表类型 1.全量 2.增量 3.变化加增量
table_type=1
##创建时间 全量表不用配置
create_time=
##更新修改时间 只有 3.变化加增量 需要配置
update_time=




case $table_type in
    1)
        sql="select $cloumns from $table_name where 1=1 "
    ;;
    2)
        sql="select $cloumns from $table_name where date_format($create_time,'%Y-%m-%d')='$do_date' "
    ;;
    3)
        sql="select $cloumns from $table_name where date_format($create_time,'%Y-%m-%d')='$do_date' or date_format($update_time,'%Y-%m-%d')='$do_date' "
    ;;
esac

echo $sql

hive=/opt/hive/bin/hive
hadoop=/opt/hadoop/bin/hadoop
sqoop=/opt/sqoop/bin/sqoop

$sqoop import \
--connect $jdbc \
--username $username \
--password $password \
--target-dir /origin_data/$db_name/db/$table_name/$do_date \
--delete-target-dir \
--query "$sql and \$CONDITIONS" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec lzop \
--null-string '\\N' \
--null-non-string '\\N'

$hadoop jar /opt/hadoop/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /origin_data/$db_name/db/$table_name/$do_date

sql="use $db_name; load data inpath '/origin_data/$db_name/db/$table_name/$do_date' OVERWRITE into table ods_$table_name partition(dt='$do_date');"

## 加载数据
$hive -e "$sql"
use gmall;
drop table if exists ods_event_log;
create external table  ods_event_log(
    line string comment 'event事件日志'
)partitioned by (dt String comment '分区日期')
stored as
    inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_event_log';

-- 加载数据
load data inpath '/origin_data/gmall/log/topic_event/2020-04-02' into table gmall.ods_event_log partition (dt='2020-04-02');
-- 创建lzo索引
hadoop jar  /opt/hadoop/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/gmall/ods/ods_event_log/dt=2020-04-02
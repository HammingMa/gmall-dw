#!/bin/bash

###### 启动 log-agent-kafka-hdfs.conf 的flume agent

cmd=$1


case $cmd in
"start"){
    echo "============启动flume agent============"
    cmds="nohup /opt/flume/bin/flume-ng agent -n a1 -c /opt/flume/conf -f /opt/dw-gmall/flume-jobs/log-agent-kafka-hdfs.conf  1>/dev/null 2>&1 &"
};;
"stop"){
    echo "============停止kafka============"
    cmds="ps -ef | awk '/log-agent-kafka-hdfs.conf/ && !/awk/ {print \$2}'|xargs kill -9"
};;
esac

echo "==============hdp3=============="
ssh hdp3 "source /etc/profile; $cmds"

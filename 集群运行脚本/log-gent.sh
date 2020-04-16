#!/bin/bash

###### 启动 log-agent-taildir-kafka.conf 的flume agent

cmd=$1


case $cmd in
"start"){
    echo "============启动flume agent============"
    cmds="nohup /opt/flume/bin/flume-ng agent -n a1 -c /opt/flume/conf -f /opt/dw-gmall/flume-jobs/log-agent-taildir-kafka.conf  1>/dev/null 2>&1 &"
};;
"stop"){
    echo "============停止kafka============"
    cmds="ps -ef | awk '/log-agent-taildir-kafka.conf/ && !/awk/ {print \$2}'|xargs kill -9"
};;
esac

for i in hdp1 hdp2 ; do
	echo "==============$i=============="
	ssh $i "source /etc/profile; $cmds"
done

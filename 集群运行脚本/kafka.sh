#!/bin/bash

###### kafka 集群启停脚本

cmd=$1

case $cmd in
"start"){
    echo "============启动kafka============"
    ./xcall2.sh /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
};;
"stop"){
    echo "============停止kafka============"
    ./xcall2.sh  /opt/kafka/bin/kafka-server-stop.sh
};;
esac
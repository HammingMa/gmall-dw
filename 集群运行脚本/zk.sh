#!/bin/bash

## zookeeper集群起脚本

cmd=$1

for i in hdp1 hdp2 hdp3; do
	echo "==============$i=============="
	ssh $i "source /etc/profile; /opt/zookeeper/bin/zkServer.sh $cmd"
done
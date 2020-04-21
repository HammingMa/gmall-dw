#!/bin/bash

## solr 启动脚本

cmd=$1

for i in hdp1 hdp2 hdp3; do
	echo "==============$i=============="
	ssh $i "source /etc/profile;  /opt/solr/bin/solr $cmd"
done
#!/bin/bash

## 执行命令脚本

cmd=$*

for i in hdp1 hdp2 hdp3; do
	echo "==============$i=============="
	ssh $i "source /etc/profile; $cmd"
done
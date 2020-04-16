#!/bin/bash

## 执行命令脚本

cmd=$*

for i in `cat ./slaves`
 do
	echo "==============$i=============="
	ssh $i "source /etc/profile; $cmd"
done
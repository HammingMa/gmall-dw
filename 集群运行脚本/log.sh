#/bin/bash


## 启动生成日志的脚本

for i in hdp1 hdp2; do
	echo "=================$i================="
	ssh $i "source /etc/profile; java -cp /opt/dw-gmall/log-producer-1.0-SNAPSHOT-jar-with-dependencies.jar com.mzh.gmallDW.appclient.AppMain $1 $2 1>/dev/null 2>&1"
done
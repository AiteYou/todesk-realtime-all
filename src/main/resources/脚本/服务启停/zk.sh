#!/bin/bash

#检验参数
if [ $# -lt 1 ]
then
	echo '参数不能为空！！！'
	exit 
fi

#循环遍历每一台机器，分别启动或者停止ZK服务

for host in zuler101 zuler102 zuler103
do
	case $1 in
		"start")
			echo "*****************start $host zookeeper****************"
			ssh $host /home/bigdata/module/zookeeper-3.5.7/bin/zkServer.sh $1
		 ;;
			
		 "stop")
			echo "*****************stop $host zookeeper****************"
			ssh $host /home/bigdata/module/zookeeper-3.5.7/bin/zkServer.sh $1
		 ;;
		  
		 "status")
			echo "*****************status $host zookeeper****************"
			ssh $host /home/bigdata/module/zookeeper-3.5.7/bin/zkServer.sh $1
		 ;;
		  
		 *)
			 echo '参数有误！！！'
			 exit
		 ;;
	esac
done

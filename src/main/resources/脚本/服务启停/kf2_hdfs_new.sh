#!/bin/bash
if [ $# -lt 1 ]
then
  echo "kf2_hdfs_new.sh {start|stop}"
  exit
fi

case $1 in
start)
   for i in zuler101
   do
          ssh $i "nohup flume-ng agent -c $FLUME_HOME/conf -f $FLUME_HOME/jobs/kafka_hdfs_new2.conf -n a1  -Dflume.root.logger=INFO,console 1>/home/bigdata/module/flume-1.9.0/logs/kafka_hdfs_new2.log 2>&1 & "
   done

;;

stop)
   for i in zuler101
   do
          ssh $i "ps -ef | grep kafka_hdfs_new2.conf | grep -v grep | awk '{print \$2}' | xargs -n1 kill -9"
   done
;;

*)
  echo "kf_2hdfs_new.sh {start|stop}"
  exit
;;
esac

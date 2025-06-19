#!/bin/bash
if [ $# -lt 1 ]
then 
  echo "Input Args Error....."
  exit
fi
for i in zuler101 zuler102 zuler103
do

case $1 in
start)
  echo "==================START $i KAFKA==================="
  ssh $i /home/bigdata/module/kafka-2.4.1/bin/kafka-server-start.sh -daemon /home/bigdata/module/kafka-2.4.1/config/server.properties
;;
stop)
  echo "==================STOP $i KAFKA==================="
  ssh $i /home/bigdata/module/kafka-2.4.1/bin/kafka-server-stop.sh stop
;;

*)
 echo "Input Args Error....."
 exit
;;  
esac

done

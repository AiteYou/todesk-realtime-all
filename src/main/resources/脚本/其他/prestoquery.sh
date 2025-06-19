#!/bin/bash
export JAVA_HOME=/home/bigdata/module/jdk1.8.0_212
export PATH=$PATH:$JAVA_HOME/bin
a=`/home/bigdata/software/presto/presto-cli-0.274 --server zuler101:8881 --user yanagishima  --execute "SELECT query_id,state,user,source,regexp_replace(query,'\n',''),resource_group_id,queued_time_ms,analysis_time_ms,created,started,last_heartbeat,\"end\" FROM system.runtime.queries" --output-format CSV >> /home/bigdata/bin/historyquery.csv`
##SELECT query_id,state,user,source,regexp_replace(query,'\n',''),resource_group_id,queued_time_ms,analysis_time_ms,created,started,last_heartbeat,"end" FROM system.runtime.queries;
echo $a

#!/bin/bash

# 定义变量方便修改
APP=todesk

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
   do_date=$1
else 
   do_date=`date -d "-1 day" +%F`
fi 

echo ================== 日志日期为 $do_date ==================
sql="
load data inpath '/origin_data/click/$do_date' into table ${APP}.ods_click_log partition(dt='$do_date');
load data inpath '/origin_data/client/$do_date' into table ${APP}.ods_client_log partition(dt='$do_date');
load data inpath '/origin_data/event/$do_date' into table ${APP}.ods_event_log partition(dt='$do_date');
load data inpath '/origin_data/now/$do_date' into table ${APP}.ods_now_log partition(dt='$do_date');
load data inpath '/origin_data/other/$do_date' into table ${APP}.ods_other_log partition(dt='$do_date');
load data inpath '/origin_data/tevent/$do_date' into table ${APP}.ods_tevent_log partition(dt='$do_date');
load data inpath '/origin_data/tuevent/$do_date' into table ${APP}.ods_tuevent_log partition(dt='$do_date');
"


hive -e "$sql"

hadoop jar /home/bigdata/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/$APP/ods/ods_tuevent_log/dt=$do_date
hadoop jar /home/bigdata/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/$APP/ods/ods_tevent_log/dt=$do_date
hadoop jar /home/bigdata/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/$APP/ods/ods_click_log/dt=$do_date
hadoop jar /home/bigdata/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/$APP/ods/ods_event_log/dt=$do_date
hadoop jar /home/bigdata/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/$APP/ods/ods_client_log/dt=$do_date
hadoop jar /home/bigdata/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/$APP/ods/ods_other_log/dt=$do_date
hadoop jar /home/bigdata/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/$APP/ods/ods_now_log/dt=$do_date


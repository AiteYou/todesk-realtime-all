#!/bin/bash

APP=todesk

if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d '-1 day' +%F`
fi

ods_daas_bill_order_detail="
load data inpath '/origin_data/daas_db/bill_order_detail/$do_date' OVERWRITE into table todesk.ods_daas_bill_order_detail partition(dt='$do_date');"
ods_daas_bill_order="
load data inpath '/origin_data/daas_db/bill_order/$do_date' OVERWRITE into table todesk.ods_daas_bill_order partition(dt='$do_date');"
ods_daas_tv_machine_connection="
load data inpath '/origin_data/daas_db/tv_machine_connection/$do_date' OVERWRITE into table todesk.ods_daas_tv_machine_connection partition(dt='$do_date');"
ods_daas_tv_todesk_user="
load data inpath '/origin_data/daas_db/tv_todesk_user/$do_date' OVERWRITE into table todesk.ods_daas_tv_todesk_user partition(dt='$do_date');"
ods_daas_tv_machine_powerchange="
load data inpath '/origin_data/daas_db/tv_machine_powerchange/$do_date' OVERWRITE into table todesk.ods_daas_tv_machine_powerchange partition(dt='$do_date');"
ods_daas_tv_user="
load data inpath '/origin_data/daas_db/tv_user/$do_date' OVERWRITE into table todesk.ods_daas_tv_user partition(dt='$do_date');"
ods_daas_try_order_virtual_desktop="
load data inpath '/origin_data/daas_db/try_order_virtual_desktop/$do_date' OVERWRITE into table todesk.ods_daas_try_order_virtual_desktop partition(dt='$do_date');"


case $1 in
    "ods_daas_bill_order_detail"){
        hive -e "$ods_daas_bill_order_detail"
    };;
    "ods_daas_bill_order"){
        hive -e "$ods_daas_bill_order"
    };;
    "ods_daas_tv_machine_connection"){
        hive -e "$ods_daas_tv_machine_connection"
    };;
    "ods_daas_tv_todesk_user"){
        hive -e "$ods_daas_tv_todesk_user"
    };;
    "ods_daas_tv_machine_powerchange"){
        hive -e "$ods_daas_tv_machine_powerchange"
    };;
    "ods_daas_tv_user"){
        hive -e "$ods_daas_tv_user"
    };;
    "ods_daas_try_order_virtual_desktop" ){
	hive -e "$ods_daas_try_order_virtual_desktop"
    };;
    "all" ){
        hive -e "$ods_daas_bill_order_detail$ods_daas_tv_machine_connection$ods_daas_tv_todesk_user$ods_daas_tv_machine_powerchange$ods_daas_tv_user$ods_daas_try_order_virtual_desktop$ods_daas_bill_order"
    };;
esac


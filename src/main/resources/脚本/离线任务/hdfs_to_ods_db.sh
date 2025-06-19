#!/bin/bash

APP=todesk

if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d '-1 day' +%F`
fi

ods_user_info=" 
load data inpath '/origin_data/db/user_info/$do_date' OVERWRITE into table ${APP}.ods_user_info partition(dt='$do_date');"

ods_mac_info="
load data inpath '/origin_data/db/mac_info/$do_date' OVERWRITE into table ${APP}.ods_mac_info partition(dt='$do_date');"

ods_online="
load data inpath '/origin_data/db/online/$do_date' OVERWRITE into table ${APP}.ods_online partition(dt='$do_date');"

ods_order_buy="
load data inpath '/origin_data/db/order_buy/$do_date' OVERWRITE into table ${APP}.ods_order_buy partition(dt='$do_date');"

ods_tv_macine="
load data inpath '/origin_data/db/tv_macine/$do_date' OVERWRITE into table ${APP}.ods_tv_macine partition(dt='$do_date');"

ods_tv_try="
load data inpath '/origin_data/db/tv_try/$do_date' OVERWRITE into table ${APP}.ods_tv_try partition(dt='$do_date');"

ods_tv_bill_order="
load data inpath '/origin_data/db/bill_order/$do_date' OVERWRITE into table ${APP}.ods_tv_bill_order partition(dt='$do_date');"

ods_tv_function_config="
load data inpath '/origin_data/db/tv_function_config/$do_date' OVERWRITE into table ${APP}.ods_tv_function_config partition(dt='$do_date');"

ods_tv_bill_order_detail="
load data inpath '/origin_data/db/tv_bill_order_detail/$do_date' OVERWRITE into table ${APP}.ods_tv_bill_order_detail partition(dt='$do_date');"

ods_tv_product="
load data inpath '/origin_data/db/tv_product/$do_date' OVERWRITE into table ${APP}.ods_tv_product partition(dt='$do_date');"

case $1 in
    "ods_user_info"){
        hive -e "$ods_user_info"
    };;
    "ods_mac_info"){
        hive -e "$ods_mac_info"
    };;
    "ods_online"){
	hive -e "$ods_online"
    };;
    "ods_user_info_test_2"){
	hive -e "$ods_user_info_test_2"
    };;
    "ods_order_buy"){
	hive -e "$ods_order_buy"
    };;
    "ods_tv_macine"){
        hive -e "$ods_tv_macine"
    };;
    "ods_tv_try"){
        hive -e "$ods_tv_try"
    };;
    "ods_tv_bill_order" )
	hive -e "$ods_tv_bill_order"
    ;;
    "ods_tv_function_config" )
	hive -e "$ods_tv_function_config"
    ;;
    "ods_tv_bill_order_detail" )
	hive -e "$ods_tv_bill_order_detail"
    ;;
    "ods_tv_product" )
	hive -e "$ods_tv_product"
    ;;
    "all" ){
        hive -e "$ods_user_info$ods_mac_info$ods_tv_try$ods_order_buy$ods_tv_bill_order$ods_tv_function_config$ods_tv_bill_order_detail$ods_tv_product" 
    };;
esac


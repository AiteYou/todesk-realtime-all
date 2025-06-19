#!/bin/bash

APP=todesk
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d "-1 day" +%F`
fi

dim_user_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dim_user_info partition (dt='$do_date')
select
    id,
    phone_num,
    email,
    user_level,
    fid,
    create_time,
    delete_time,
    last_time
from ${APP}.ods_user_info
where dt='$do_date'
union
select
    id,
phone_num,
email,
user_level,
fid,
create_time,
delete_time,
last_time
from ${APP}.dim_user_info
where dt=date_add('$do_date',-1);"

dim_mac_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dim_mac_info partition (dt='$do_date')
select 
	nvl(t1.id,t0.id),
	nvl(t1.mac_id,t0.mac_id),
	nvl(t1.client_type,t0.client_type),
	nvl(t1.create_time,t0.create_time)
from
(
	select
    id,
    mac_id,
    client_type,
    create_time
from ${APP}.dim_mac_info
where dt=date_add('$do_date',-1)
)t0
full outer join
(
select
    id,
    mac_id,
    min(cast(client_type as int )) client_type,
    create_time
from ${APP}.ods_mac_info
where dt='$do_date'
group by id,mac_id,create_time
)t1
on t0.mac_id=t1.mac_id;"

dim_su_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dim_su_info partition (dt='$do_date')
select
    suid,
    sclient,
    min(ts)
from ${APP}.dwd_new_connect
where dt=date_add('$do_date',-1)
group by suid,sclient
union
select
    suid,
    sclient,
    min(ts)
from ${APP}.dwd_new_connect
where dt='$do_date'
group by suid,sclient;"

dim_du_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dim_du_info partition (dt='$do_date')
select
    duid,
    dclient,
    min(ts) c1
from ${APP}.dwd_new_connect
where dt=date_add('$do_date',-1)
group by duid,dclient
union
select
    duid,
    dclient,
    min(ts) c1
from ${APP}.dwd_new_connect
where dt='$do_date'
group by duid,dclient;"

dwd_tv_try="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_tv_try partition (dt='$do_date')
select
    id,userid,activity_id,type,value,day,status,level,param,start_time,end_time,create_time,update_time
from ${APP}.ods_tv_try
where dt='$do_date';"

dwd_order_buy="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_order_buy partition (dt='$do_date')
select
    user_id, vip_level,pay_time
from ${APP}.ods_order_buy
where dt='$do_date';"

dwd_tv_macine="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_tv_macine partition (dt='$do_date')
select
    user_id,mac_id,login_in from ${APP}.ods_tv_macine where dt='$do_date';"

dwd_tv_function_config="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_tv_function_config partition (dt='$do_date')
select id,order_id,user_id,mac_id,spu_id,status,type,value,level,start_time,end_time,create_time,update_time from ${APP}.ods_tv_function_config where dt='$do_date';"

dwd_tv_bill_order_detail="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_tv_bill_order_detail partition (dt='$do_date')
select id,userid,macid,order_id,product_id,refund_id,cycle_agreement_detail_id,renewal_times,desc,upgrade_order_id,amount,order_type,params,status,start_time,end_time,total_price,actual_price,total_fee,create_time,update_time,original_id,charge_type from ${APP}.ods_tv_bill_order_detail where dt='$do_date';"


dwd_tv_product="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_tv_product partition (dt='$do_date')
select  id,spu_id from ${APP}.ods_tv_product where dt='$do_date';"

dwd_daas_tv_machine_connection="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_daas_tv_machine_connection partition (dt='$do_date')
select
    id, connection_id, tdid, tduid, to_tdid, connected_time, disconnected_time, connection_type, td_name, to_name, company_id, virtual_desktop_id, td_ip, to_ip, email, group_name
from ${APP}.ods_daas_tv_machine_connection
where dt='$do_date';"

dwd_daas_tv_machine_powerchange="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_daas_tv_machine_powerchange partition (dt='$do_date')
select
    id, connection_id, tduid, poweron_time, poweroff_time, virtual_desktop_id, email, company_id
from ${APP}.ods_daas_tv_machine_powerchange
where dt='$do_date';"

case $1 in
    dim_user_info )
      hive -e "$dim_user_info"
    ;;
    dim_mac_info )
      hive -e "$dim_mac_info"
    ;;
    dim_su_info )
      hive -e "$dim_su_info"
    ;;
    dim_du_info )
      hive -e "$dim_du_info"
    ;;
    dwd_tv_try )
	hive -e "$dwd_tv_try"
    ;;
    dwd_tv_function_config )
	hive -e "$dwd_tv_function_config"
    ;;
    dwd_tv_bill_order_detail )
	hive -e "$dwd_tv_bill_order_detail"
    ;;
    dwd_tv_macine )
	hive -e "$dwd_tv_macine"
    ;;
    dwd_tv_product )
	hive -e "$dwd_tv_product"
    ;;
    all )
     hive -e "$dim_user_info$dim_mac_info$dwd_tv_try$dwd_order_buy$dwd_tv_function_config$dwd_tv_bill_order_detail$dwd_tv_product$dwd_daas_tv_machine_connection$dwd_daas_tv_machine_powerchange"
    ;;
esac

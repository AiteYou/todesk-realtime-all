#! /bin/bash

APP_MYSQL=zuler_bussiness_center
APP_HADOOP=daas
sqoop=/home/bigdata/module/sqoop-1.4.6/bin/sqoop
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
   do_date=$2
   do_date_online=`date -d $2 +%Y%m%d`
else
   do_date=`date -d "-1 day" +%F`
   do_date_online=`date -d "-1 day" +%Y%m%d`
fi

#--connect jdbc:mysql://gz-cdbrg-nmkcpn37.sql.tencentcdb.com:56806/$APP_MYSQL \
import_data(){
$sqoop import \
--connect jdbc:mysql://gz-cdbrg-jeg8v4rt.sql.tencentcdb.com:23161/$APP_MYSQL?autoReconnect=true \
--username root \
--password YJYgEHzGCU58Wgry \
--target-dir /origin_data/daas_db/$1/$do_date \
--delete-target-dir \
--query "$2 and \$CONDITIONS" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec lzop \
--outdir "/home/bigdata/bin/import.sqoop" \
--null-string '\\N' \
--null-non-string '\\N'
--jar-file mysql-connector-java-8.0.30.jar

hadoop jar /home/bigdata/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /origin_data/daas_db/$1/$do_date
}

import_bill_order_detail() {
        import_data "bill_order_detail" "
        select id,concat(env,''),userid,macid,order_id,product_id,spu_id,product_price_id,cycle_agreement_detail_id,
renewal_times,upgrade_order_id,amount,concat(order_type,''),params,concat(status,''),buy_time,start_time,end_time,use_start_time,use_end_time,total_price,actual_price,total_fee,create_time,update_time from bill_order_detail
	where 1=1"
}

import_bill_order() {
	import_data "bill_order" "
	select id,concat(env,''),userid,coupon_id,activity_id,extra_id,cycle_agreement_id,product_price_id,concat(channel,''),concat(status,''),ip,agreement_no,concat(order_type,''),trade_id,transaction_id,coupon_price,total_price,actual_price,total_fee,start_time,end_time,concat(pay_status,''),pay_time,concat(is_invoice,''),invoice_id,concat(is_refund,''),concat(is_extract,''),platform,create_time,update_time from bill_order where 1=1"

}

import_tv_machine_connection() {
	import_data "tv_machine_connection" "
	select id,connection_id,tdid,tduid,to_tdid,connected_time,disconnected_time,concat(connection_type,''),td_name,to_name,company_id,virtual_desktop_id,td_ip,to_ip,email,group_name from tv_machine_connection where 1=1"
}

import_tv_todesk_user() {
	import_data "tv_todesk_user" "
	select id,userid,todesk_userid,create_time,update_time from tv_todesk_user where 1=1"
}

import_tv_machine_powerchange() {
	import_data "tv_machine_powerchange" "
	select id,connection_id,tduid,poweron_time,poweroff_time,virtual_desktop_id,email,company_id from tv_machine_powerchange where 1=1"
}

import_tv_user() {
	import_data "tv_user" "
	select id,phone,regdate,active_date,concat(status,''),is_delete,gold from tv_user where 1=1"
}

import_try_order_virtual_desktop() {
	import_data "try_order_virtual_desktop" "
	select id,cluster_id,product_id,host_id,activity_id,product_vm_id,concat(charge_type,''),remaining_time,desktop_id,name,spu_id,userid,concat(customer,''),concat(status,''),concat(power_status,''),create_time,update_time,time_config_id from try_order_virtual_desktop where 1=1"
}
case $1 in
  "bill_order_detail" )
        import_bill_order_detail
;;
  "bill_order" )
        import_bill_order
;;
  "tv_machine_connection" )
	import_tv_machine_connection
;;
 "tv_todesk_user" )
	import_tv_todesk_user
;;
 "tv_machine_powerchange" )
	import_tv_machine_powerchange
;;
 "tv_user" )
	import_tv_user
;;
"try_order_virtual_desktop" )
	import_try_order_virtual_desktop
;;
  "all" )
     import_bill_order
     import_bill_order_detail
     import_tv_machine_connection
     import_tv_todesk_user
     import_tv_machine_powerchange
     import_tv_user
     import_try_order_virtual_desktop
;;
esac


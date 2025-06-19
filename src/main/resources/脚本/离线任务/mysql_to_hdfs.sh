#! /bin/bash

APP_MYSQL=center
APP_HADOOP=todesk
sqoop=/home/bigdata/module/sqoop-1.4.6/bin/sqoop
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
   do_date=$2
   do_date_online=`date -d $2 +%Y%m%d`
else 
   do_date=`date -d "-1 day" +%F`
   do_date_online=`date -d "-1 day" +%Y%m%d`
fi 

#-D yarn.app.mapreduce.am.resource.mb=1024 \
#-D yarn.scheduler.minimum-allocation-mb=8096 \
#-D yarn.scheduler.maximum-allocation-mb=16192 \
#-D mapreduce.map.memory.mb=8096 \
#-D mapreduce.reduce.memory.mb=8096 \

#--connect jdbc:mysql://gz-cdbrg-nmkcpn37.sql.tencentcdb.com:56806/$APP_MYSQL \
import_data(){
$sqoop import \
--connect jdbc:mysql://gz-cdbrg-frwg2xfv.sql.tencentcdb.com:63850/$APP_MYSQL?autoReconnect=true \
--username root \
--password fya12345FYD \
--target-dir /origin_data/db/$1/$do_date \
--delete-target-dir \
--query "$2 and \$CONDITIONS" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec lzop \
--outdir "/home/bigdata/bin/import.sqoop" \
--null-string '\\N' \
--null-non-string '\\N'

hadoop jar /home/bigdata/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /origin_data/db/$1/$do_date
}

import_login(){
$sqoop import \
--connect jdbc:mysql://106.75.147.204:11225/Todesk_Count?tinyInt1isBit=false \
--username root \
--password 7mGC4LHqnZFEwseR \
--target-dir /origin_data/db/$1/$do_date \
--delete-target-dir \
--query "$2 and \$CONDITIONS" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec lzop \
--outdir "/home/bigdata/bin/import.sqoop" \
--null-string '\\N' \
--null-non-string '\\N'

hadoop jar /home/bigdata/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /origin_data/db/$1/$do_date
}

user(){
$sqoop import \
--connect jdbc:mysql://106.75.147.204:11223/$APP_MYSQL \
--username root \
--password fya12345FYD \
--target-dir /origin_data/db/$1/$do_date \
--delete-target-dir \
--query "$2 and \$CONDITIONS" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec lzop \
--outdir "/home/bigdata/bin/import.sqoop" \
--null-string '\\N' \
--null-non-string '\\N'

}

timeStamp=`date -d "$do_date" +%s`
import_user_info(){
  import_data user_info "select 
                            id,
                            phone,
                            email, 
                            viplevel,
                            fid,
                            regdate,
                            delete_time,
                            lasttime
                          from tv_user
                          where  regdate>=$timeStamp"
}

import_mac_info(){
  import_data "mac_info" "select 
                            id,
                            macid,
                            clienttype,
                            regdate
                          from tv_mac
                          where  regdate >=$timeStamp and regdate<$timeStamp+86400"
}

import_order_buy() {
  import_data "order_buy" "select
			     id,
                             viplevel,
			     pay_time,
			     state
                             from tv_order_buy
                             where 1=1"
}

import_tv_package(){
  import_data tv_package "select 
                            userid,
                            rps,
                            tps, 
                            gnum
                          from tv_package
                          where  1=1"
}

import_tv_macine(){
  import_data tv_macine "select 
                            user,macid,loginin
                          from tv_macine
                          where  1=1"
}

import_tv_try(){
 import_data tv_try "select
			id,userid,activity_id,type,value,day,status,level,params,start_time,end_time,create_time,update_time
			from tv_try_function_config
			where 1=1"
}

import_bill_order() {
	import_data "bill_order" "
		select  id,userid,trade_id,total_fee,pay_time,CONCAT(channel,''),CONCAT(order_type,''),CONCAT(status,''),CONCAT(pay_status,''),CONCAT(is_invoice,'')  FROM tv_bill_order where 1=1
	"
}

import_tv_function_config() {
	import_data "tv_function_config" "
	select
	    id,order_detail_id,userid,macid,spu_id,CONCAT(status,''),type,value,level,start_time,end_time,create_time,update_time
	from tv_function_config
	where 1=1
	"
}

import_tv_bill_order_detail() {
	import_data "tv_bill_order_detail" "
	select 
		id ,userid ,macid ,order_id ,product_id ,refund_id ,cycle_agreement_detail_id ,renewal_times ,\`desc\` ,upgrade_order_id ,amount ,order_type ,params ,
case when status=0 then 'a'
         when status=1 then 'b'
         when status=2 then 'c'
         when status=3 then 'd'
    else 'other' end  ,start_time ,end_time ,total_price ,actual_price ,total_fee ,create_time ,update_time ,original_id ,charge_type 
	from tv_bill_order_detail
	where 1=1"
}

import_tv_product() {
	import_data "tv_product" "
	select id,spu_id from tv_product where 1=1"
}

case $1 in
"user" )
     user_in
;;  
"tv_package" )
     import_tv_package
;;
  "tv_macine" )
     import_tv_macine
;;
  "user_info" )
     import_user_info
;;
  "mac_info" )
     import_mac_info
;;
  "online" )
     import_online
;;
  "order_buy" )
     import_order_buy
;;
  "tv_try" )
     import_tv_try
;;
  "tv_try_tmp" )
     import_tv_try_tmp
;;
  "bill_order" )
     import_bill_order
;;
   "tv_function_config" )
	import_tv_function_config
;;
   "tmp" )
	import_tmp
;;
  "tv_bill_order_detail" )
	import_tv_bill_order_detail
;;
  "tv_product" )
	import_tv_product
;;
  "all" )
     import_user_info
     import_mac_info
     import_tv_try
     import_order_buy
     import_bill_order
     import_tv_function_config
     import_tv_bill_order_detail
     import_tv_product
;;
esac

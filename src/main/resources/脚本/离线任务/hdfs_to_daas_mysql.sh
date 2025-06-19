#!/bin/bash

hive_db_name=todesk
mysql_db_name=daas_stat

export_data() {
/home/bigdata/module/sqoop-1.4.6/bin/sqoop export \
--connect "jdbc:mysql://gz-cdb-3du1v627.sql.tencentcdb.com:26119/${mysql_db_name}?useUnicode=true&characterEncoding=utf-8"  \
--username root \
--password YJYgEHzGCU58Wgry \
--table $1 \
--num-mappers 1 \
--export-dir /warehouse/$hive_db_name/ads/$1 \
--input-fields-terminated-by "\t" \
--update-mode allowinsert \
--update-key $2 \
--outdir "/home/bigdata/bin/export.sqoop" \
--input-null-string '\\N'    \
--input-null-non-string '\\N' 
}

export_data1() {
/home/bigdata/module/sqoop-1.4.6/bin/sqoop export \
--connect "jdbc:mysql://gz-cdb-3du1v627.sql.tencentcdb.com:26119/${mysql_db_name}?useUnicode=true&characterEncoding=utf-8"  \
--username root \
--password YJYgEHzGCU58Wgry \
--table $1 \
--columns dt,user_cnt,conn_cnt \
--num-mappers 1 \
--export-dir /warehouse/$hive_db_name/ads/$1 \
--input-fields-terminated-by "\t" \
--update-mode allowinsert \
--update-key $2 \
--outdir "/home/bigdata/bin/export.sqoop" \
--input-null-string '\\N'    \
--input-null-non-string '\\N' 
}

export_data2() {
/home/bigdata/module/sqoop-1.4.6/bin/sqoop export \
--connect "jdbc:mysql://gz-cdb-3du1v627.sql.tencentcdb.com:26119/${mysql_db_name}?useUnicode=true&characterEncoding=utf-8"  \
--username root \
--password YJYgEHzGCU58Wgry \
--table $1 \
--columns dt,flag,register_date,retention_day,retention_user_count,register_count,retention_user_rate \
--num-mappers 1 \
--export-dir /warehouse/$hive_db_name/ads/$1 \
--input-fields-terminated-by "\t" \
--update-mode allowinsert \
--update-key $2 \
--outdir "/home/bigdata/bin/export.sqoop" \
--input-null-string '\\N'    \
--input-null-non-string '\\N' 
}

export_data3() {
/home/bigdata/module/sqoop-1.4.6/bin/sqoop export \
--connect "jdbc:mysql://gz-cdb-3du1v627.sql.tencentcdb.com:26119/${mysql_db_name}?useUnicode=true&characterEncoding=utf-8"  \
--username root \
--password YJYgEHzGCU58Wgry \
--table $1 \
--columns $3  \
--num-mappers 1 \
--export-dir /warehouse/$hive_db_name/ads/$1 \
--input-fields-terminated-by "\t" \
--update-mode allowinsert \
--update-key $2 \
--outdir "/home/bigdata/bin/export.sqoop" \
--input-null-string '\\N'    \
--input-null-non-string '\\N'   
}
case $1 in 
"ads_daas_selling_time_total" )
	export_data "ads_daas_selling_time_total" "dt,is_todesk,product_price_id"
;;
"ads_daas_used_time_total" )
	export_data "ads_daas_used_time_total" "dt,product_price_id,flag"
;;
ads_daas_user_total )
	export_data1 "ads_daas_user_total" "dt"
;;
"ads_daas_user_retention" )
	export_data2 "ads_daas_user_retention" "flag,register_date,retention_day"
;;
"ads_daas_pay_total" )
	export_data "ads_daas_pay_total" "dt,is_todesk,is_renewal,product_price_id"
;;
"ads_daas_order_total" )
	export_data "ads_daas_order_total" "dt,is_todesk,is_renewal,product_price_id"
;;
"ads_todesk_province_total" )
	export_data "ads_todesk_province_total" "dt,flag,province"
;;
"test" )
	export_data3 "ads_cloud_new_user_total" "dt,d_labtype,d_client" "dt,d_labtype,d_client,daas_cnt,todesk_login_cnt,total_cnt"
	export_data3 "ads_cloud_paying_user_total" "dt,d_labtype,d_renewal,d_client" "dt,d_labtype,d_renewal,d_client,daas_cnt,todesk_login_cnt,total_cnt"
	export_data3 "ads_cloud_connected_user_total" "dt,d_labtype,d_card,d_client" "dt,d_labtype,d_card,d_client,user_active_cnt,user_connect_cnt"
;;
cc )
	export_data3 "ads_cloud_connected_user_total" "dt,d_labtype,d_card" "dt,d_labtype,d_card,user_active_cnt,user_connect_cnt"
;;
all ) 
	export_data "ads_daas_selling_time_total" "dt,is_todesk,product_price_id"
export_data1 "ads_daas_user_total" "dt"
export_data2 "ads_daas_user_retention" "flag,register_date,retention_day"
export_data "ads_daas_user_gold_total" "dt,gold"
export_data "ads_daas_newuser_total" "dt,platform"
export_data3 "ads_daas_user_distributed_total" "dt" "dt,daas_cnt,todesk_cnt,todesk_login_cnt,total_cnt"
export_data3 "ads_cloud_new_user_total" "dt,d_labtype,d_client" "dt,d_labtype,d_client,daas_cnt,todesk_login_cnt,total_cnt"
        export_data3 "ads_cloud_paying_user_total" "dt,d_labtype,d_renewal,d_client" "dt,d_labtype,d_renewal,d_client,daas_cnt,todesk_login_cnt,total_cnt"
        export_data3 "ads_cloud_connected_user_total" "dt,d_labtype,d_card,d_client" "dt,d_labtype,d_card,d_client,user_active_cnt,user_connect_cnt"
;;
esac

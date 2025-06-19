#!/bin/bash

hive_db_name=todesk
mysql_db_name=gaas_stat

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

case $1 in
ads_gaas_paying_user_total )
	export_data "ads_gaas_paying_user_total" "dt,order_type"
;;
ads_gaas_free_user_retention )
	export_data "ads_gaas_free_user_retention" "register_date,retention_day"
;;
ads_gaas_paying_user_retention )
	export_data "ads_gaas_paying_user_retention" "register_date,retention_day"
;;
ads_gaas_active_user_total )
	export_data "ads_gaas_active_user_total" "dt,new_user"
;;
ads_gaas_active_user_retention )
	export_data "ads_gaas_active_user_retention" "dt,new_user,retention_day"
;;
ads_gaas_active_mac_total )
	export_data "ads_gaas_active_mac_total" "dt,plat"
;;
test )
	export_data "ads_gaas_free_user_retention" "register_date,retention_day"
        export_data "ads_gaas_paying_user_retention" "register_date,retention_day"
;;	
all )
	export_data "ads_gaas_paying_user_total" "dt,order_type,platform"
	export_data "ads_gaas_free_user_retention" "register_date,retention_day"
	export_data "ads_gaas_paying_user_retention" "register_date,retention_day"
	export_data "ads_gaas_active_user_total" "dt,new_user"
	export_data "ads_gaas_active_user_retention" "dt,new_user,retention_day"
	export_data "ads_gaas_active_mac_total" "dt,plat"
	export_data "ads_gaas_game_total" "dt,game"
;;
esac

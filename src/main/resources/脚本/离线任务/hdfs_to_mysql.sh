#!/bin/bash

hive_db_name=todesk
mysql_db_name=todesk_report

export_data() {
/home/bigdata/module/sqoop-1.4.6/bin/sqoop export \
--connect "jdbc:mysql://zuler101:3306/${mysql_db_name}?useUnicode=true&characterEncoding=utf-8"  \
--username root \
--password wjhwdbRM7E\&E^cy^ \
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

export_data_1() {
/home/bigdata/module/sqoop-1.4.6/bin/sqoop export \
--connect "jdbc:mysql://zuler101:3306/${mysql_db_name}?useUnicode=true&characterEncoding=utf-8"  \
--username root \
--password wjhwdbRM7E\&E^cy^ \
--table $1 \
--num-mappers 1 \
--export-dir /warehouse/$hive_db_name/dwd/$1 \
--input-fields-terminated-by "\t" \
--update-mode allowinsert \
--outdir "/home/bigdata/bin/export.sqoop" \
--input-null-string '\\N'    \
--input-null-non-string '\\N'
}

export_data_2() {
/home/bigdata/module/sqoop-1.4.6/bin/sqoop export \
--connect "jdbc:mysql://192.168.1.52:3306/todesksc?useUnicode=true&characterEncoding=utf-8"  \
--username root \
--password zuler0607 \
--table $1 \
--num-mappers 1 \
--export-dir /warehouse/$hive_db_name/ads/$1 \
--input-fields-terminated-by "\t" \
--update-mode allowinsert \
--outdir "/home/bigdata/bin/export.sqoop" \
--input-null-string '\\N'    \
--input-null-non-string '\\N'
}


case $1 in
  "ads_session_day" )
    export_data "ads_session_day" "dt,enterprise"
  ;;

  "ads_conn_duration" )
    export_data "ads_conn_duration" "dt,enterprise"
  ;;
  "ads_su_client_version_total" )
    export_data "ads_su_client_version_total" "dt,client_type,version"
  ;;
  "ads_du_client_version_total" )
    export_data "ads_du_client_version_total" "dt,client_type,version"
  ;; 
  "ads_client_connect" )
    export_data "ads_client_connect" "dt"
  ;;

 "ads_client_closed" )
    export_data "ads_client_closed" "dt"
  ;;

  "ads_user_logout" )
    export_data "ads_user_logout" "dt"
  ;;
  "ads_add_device" )
    export_data "ads_add_device" "dt"
  ;;
  "ads_detele_device" )
    export_data "ads_delete_device" "dt"
  ;;
  "ads_edit_device" )
    export_data "ads_edit_device" "dt"
  ;;
  "ads_add_group" )
    export_data "ads_add_group" "dt"
  ;;
  "ads_delete_group" )
    export_data "ads_delete_group" "dt"
  ;;
  "ads_edit_group" )
    export_data "ads_edit_group" "dt"
  ;;
  "ads_android_logic_total" )
    export_data "ads_android_logic_total" "dt"
  ;;
  "ads_android_connect_total" )
    export_data "ads_android_connect_total" "dt"
  ;;
  "ads_phone_new_user" )
    export_data "ads_phone_new_user" "dt,platform"
  ;;
  "ads_android_su_action" )
   export_data "ads_android_su_action" "dt,action_type"
  ;;
  "ads_android_bus" )
   export_data "ads_android_bus" "dt,bus"
  ;;
  "ads_android_resolution" )
   export_data "ads_android_resolution" "dt,resolution"
  ;;
  "ads_android_bios" )
   export_data "ads_android_bios" "dt,platform,vendor,bios"
  ;;
  "ads_android_signin_total" )
   export_data "ads_android_signin_total" "dt"
  ;;
  "ads_sclient_session" )
   export_data "ads_sclient_session" "dt,sclient_type"
  ;;
  "ads_phone_user_version" )
   export_data "ads_phone_user_version" "dt,client_type,version"
  ;;
  "ads_rtc_conn_total" )
   export_data "ads_rtc_conn_total" "dt,connect_type"
  ;;
  "ads_rtc_vedio_total" )
   export_data "ads_rtc_vedio_total" "dt,connect_type"
  ;;
  "ads_rtc_conn_fst_time" )
   export_data "ads_rtc_conn_fst_time" "dt,connect_type"
  ;;
  "ads_rtc_cap_send" )
   export_data "ads_rtc_cap_send" "dt,connect_type"
  ;;
  "ads_rtc_rcvd_rend" )
   export_data "ads_rtc_rcvd_rend" "dt,connect_type"
  ;;
  "ads_rtc_du_rate" )
   export_data "ads_rtc_du_rate" "dt,connect_type"
  ;;
  "ads_rtc_rtt" )
   export_data "ads_rtc_rtt" "dt,connect_type"
  ;;
  "ads_rtc_loss" )
   export_data "ads_rtc_loss" "dt,connect_type"
  ;;
  "ads_bus_new_user" )
        export_data "ads_bus_new_user" "dt,bus"
    ;;
    "ads_bus_user_start" )
        export_data "ads_bus_user_start" "dt,bus"
    ;;
    "ads_bus_user_retention" )
        export_data "ads_bus_user_retention" "bus,action_type,create_time,retention_day"
    ;;
    "ads_new_user" )
        export_data "ads_new_user" "dt"
    ;;
    "ads_start_user" )
        export_data "ads_start_user" "dt,action_type"
    ;;
    "ads_user_retention" )
        export_data "ads_user_retention" "action_type,create_time,retention_day"
    ;;
    "ads_client_new_mac" )
        export_data "ads_client_new_mac" "dt,client_type"
    ;;
    "ads_client_start_mac" )
        export_data "ads_client_start_mac" "dt,client_type,action_type"
    ;;
    "ads_mac_retention" )
        export_data "ads_mac_retention" "client_type,action_type,create_time,retention_day"
    ;;
    "ads_client_new_su" )
        export_data "ads_client_new_su" "dt,client_type"
    ;;
    "ads_client_start_su" )
        export_data "ads_client_start_su" "dt,client_type,action_type"
    ;;
    "ads_su_retention" )
        export_data "ads_su_retention" "client_type,action_type,create_time,retention_day"
    ;;
    "ads_client_new_du" )
        export_data "ads_client_new_du" "dt,client_type"
    ;;
    "ads_client_start_du" )
        export_data "ads_client_start_du" "dt,client_type,action_type"
    ;;
    "ads_du_retention" )
        export_data "ads_du_retention" "client_type,action_type,create_time,retention_day"
    ;;
    "ads_version_mac_total" )
        export_data "ads_version_mac_total" "dt,version"
    ;;
    "ads_su_handshake_total" )
        export_data "ads_su_handshake_total" "dt,version"
    ;;
    "ads_du_handshake_total" )
        export_data "ads_du_handshake_total" "dt,version"
    ;;
    "ads_su_handshake_failure" )
        export_data "ads_su_handshake_failure" "dt,action,version,errno"
    ;;
    "ads_du_handshake_failure" )
        export_data "ads_du_handshake_failure" "dt,action,version,errno"
    ;;
    "ads_user_login_total" )
	export_data "ads_user_login_total" "dt,login_type,enterprise"
    ;;
    "ads_vendor_process_total" )
	export_data "ads_vendor_process_total" "dt,vendor"
    ;;
    "ads_ip_user_login_total" )
	export_data "ads_ip_user_login_total" "dt,login_type,enterprise"
    ;;
    "ads_windows_first_frame_total" )
	export_data "ads_windows_first_frame_total" "dt,version,forward,action"
    ;;
    "ads_android_action_total" )
	export_data "ads_android_action_total" "dt,platform,version,action"
    ;;
    "ads_android_action_pvuv" )
	export_data "ads_android_action_pvuv" "dt,platform,version,action_type"
    ;;
    "ads_android_du_total" )
	export_data "ads_android_du_total" "dt,version"
    ;;
    "ads_android_setting_total" )
	export_data "ads_android_setting_total" "dt,version,platform"
    ;;
    "ads_android_test_double" )
	export_data "ads_android_test_double" "dt,test_double_value"
    ;;
    "ads_client_version_retention" )
	export_data "ads_client_version_retention" "first_su_date,client_type,retention_day"
    ;;
    "ads_client_version_new_su" )
	export_data "ads_client_version_new_su" "dt,client_type"
    ;;
    "ads_client_version_du_retention" )
        export_data "ads_client_version_du_retention" "first_du_date,client_type,retention_day"
    ;;
    "ads_client_version_du_total" )
        export_data "ads_client_version_du_total" "dt,client_type"
    ;;
    "ads_client_logic_total" )
	export_data "ads_client_logic_total" "dt,client_type"
    ;;
    "ads_client_logic_retention" )
	export_data "ads_client_logic_retention" "first_logic_date,client_type,retention_day" 
    ;;
    "ads_rtc_version_conn_total" )
	export_data "ads_rtc_version_conn_total" "dt,version,connect_type"
    ;;
    "ads_rtc_version_vedio_total" )
	export_data "ads_rtc_version_vedio_total" "dt,version,connect_type,local_proto"
    ;;
    "ads_rtc_version_fallback_total" )
	export_data "ads_rtc_version_fallback_total" "dt,version"
    ;;
    "ads_rtc_version_conn_fst_time" )
	export_data "ads_rtc_version_conn_fst_time" "dt,version,connect_type,local_proto"
    ;;
    "ads_rtc_version_cap_send" )
	export_data "ads_rtc_version_cap_send" "dt,version,connect_type,local_proto"
    ;;
    "ads_rtc_version_rcvd_rend" )
	export_data "ads_rtc_version_rcvd_rend" "dt,version,connect_type,local_proto"
    ;;
    "ads_rtc_version_du_rate" )
	export_data "ads_rtc_version_du_rate" "dt,version,connect_type,local_proto"
    ;;
    "ads_rtc_version_rtt" )
	export_data "ads_rtc_version_rtt" "dt,version,connect_type,local_proto"
    ;;
    "ads_rtc_version_loss" )
	export_data "ads_rtc_version_loss" "dt,version,connect_type,local_proto"
    ;;
    "ads_android_login_total" )
	export_data "ads_android_login_total" "dt,login_type,version"
    ;;
    "ads_licode_session_day" )
	export_data "ads_licode_session_day" "dt"
    ;;
    "ads_client_licode_session_day" )
	export_data "ads_client_licode_session_day" "dt,sclient,dclient,sversion,dversion"
    ;;
    "ads_web_setting_total" )
	export_data "ads_web_setting_total" "dt"
    ;;
    "ads_su_province_total" )
	export_data "ads_su_province_total" "dt,iso_code_3166,customer_type"
    ;;
    "ads_android_licode_total" )
	export_data "ads_android_licode_total" "dt,version,turndata_useLicode,licode_tyoe"
    ;;
    "ads_ios_licode_total" )
	export_data "ads_ios_licode_total" "dt,version,turndata_useLicode,licode_tyoe"
    ;;
    "ads_licode_sdk_total" )
	export_data "ads_licode_sdk_total" "dt,version"
    ;;
    "ads_licode_sig_total" )
	export_data "ads_licode_sig_total" "dt,sdk_version,url"
    ;;
    "ads_licode_ice_total" )
	export_data "ads_licode_ice_total" "dt,sdk_version,ice_connection_type"
    ;;
    "ads_ios_login_total" )
	export_data "ads_ios_login_total" "dt,login_type,version"
    ;;
    "ads_android_link_total" )
	export_data "ads_android_link_total" "dt,su_platform,version,du_platform"
    ;;
    "ads_android_forward_total" )
	export_data "ads_android_forward_total" "dt,platform,version,type"
    ;;
    "ads_windows_first_frame_fail" )
	export_data "ads_windows_first_frame_fail" "dt,version,conn_type,failure_type"
    ;;
    "ads_windows_first_frame_success" )
	export_data "ads_windows_first_frame_success" "dt,version,conn_type,time_type"
    ;;
    "ads_android_first_frame_total" )
    export_data "ads_android_first_frame_total" "dt,platform,version,type,abflag"
;;
"ads_android_vedio_total_track" )
    export_data "ads_android_vedio_total_track" "dt,platform,version"
;;
"ads_android_type_vedio_total_rtc" )
    export_data "ads_android_type_vedio_total_rtc" "dt,platform,version"
;;
"ads_android_vedio_total_rtc" )
    export_data "ads_android_vedio_total_rtc" "dt,platform,version"
;;
"ads_android_decoder_total" )
    export_data "ads_android_decoder_total" "dt,platform,version,vendor,bios,decoder_implementation"
;;
"ads_android_cap_send_total" )
    export_data "ads_android_cap_send_total" "dt,version,conn_type"
;;
"ads_licode_conn_time_total" )
    export_data "ads_licode_conn_time_total" "dt,labtype,biztype,is_file_transport"
;;
"ads_version_new_connect_total" )
    export_data "ads_version_new_connect_total" "dt,labtype,client,version"
;;
"ads_version_new_connect_clientype_total" )
    export_data "ads_version_new_connect_clientype_total" "dt,clientype,version"
;;
"ads_backend_licode_loss_total" )
    export_data "ads_backend_licode_loss_total" "dt,type,metadata_type"
;;
"ads_windows_clarity_total" )
    export_data "ads_windows_clarity_total" "dt,type"
;;
"ads_id_request_phone_total" )
	export_data "ads_id_request_phone_total" "dt,type"
;;
"ads_version_new_connect_type_total" )
	export_data "ads_version_new_connect_type_total" "dt,du_type,dversion"
;;
"ads_android_forward_fallback_total" )
	export_data "ads_android_forward_fallback_total" "dt,platform,version,action"
;;
"ads_android_logic_topic_total" )
	export_data "ads_android_logic_topic_total" "dt,client_type,bus"
;;
"ads_android_logic_retention" )
	export_data "ads_android_logic_retention" "first_logic_date,client_type,bus,retention_day"
;;
"ads_connection_total" )
	export_data "ads_connection_total" "dt,su_version,du_version,enterprise"
;;
"ads_p2p_network_total" )
	export_data "ads_p2p_network_total" "dt,client_network,host_network,client_version,host_version"
;;
"ads_licode_connection_action" )
	export_data "ads_licode_connection_action" "dt"
;;
"ads_backend_ec_error_reason_all" )
	export_data "ads_backend_ec_error_reason_all" "dt,reason"
;;
"ads_backend_ec_error_reason_both" )
	export_data "ads_backend_ec_error_reason_both" "dt,reason"
;;
"ads_rtc_switch_total" )
        export_data "ads_rtc_switch_total" "dt,du_version"
;;
"ads_backend_ea_error_reason_all" )
        export_data "ads_backend_ea_error_reason_all" "dt,reason"
;;
"ads_backend_ea_error_reason_both" )
        export_data "ads_backend_ea_error_reason_both" "dt,reason"
;;
"ads_serv_diff_total" )
	export_data "ads_serv_diff_total" "dt,ads_serv_diff_total"
;;
"ads_reconnect_session_day" )
	export_data "ads_reconnect_session_day" "dt,reconnect"
;;
"ads_client_newuser_newsu" )
        export_data "ads_client_newuser_newsu" "dt,client_type"
;;
"ads_client_newuser_suretention" )
        export_data "ads_client_newuser_suretention" "first_su_date,client_type,retention_day"
;;
 "ads_client_newuser_newdu" )
        export_data "ads_client_newuser_newdu" "dt,client_type"
;;
"ads_client_newuser_duretention" )
        export_data "ads_client_newuser_duretention" "first_du_date,action_type,retention_day"
;;
"ads_android_du_cap_send_total" )
	export_data "ads_android_du_cap_send_total" "dt,du_client_type,client_type,version,conn_type"
;;
"ads_bigdata_datas_total" )
	export_data "ads_bigdata_datas_total" "dt,log_type"
;;
"ads_rtc_rpru_total" )
	export_data "ads_rtc_rpru_total" "dt,du_version"
;;
"ads_pc_new_version_total" )
	export_data "ads_pc_new_version_total" "dt,version"
;;
"ads_licode_connection_action_4500" )
    export_data "ads_licode_connection_action_4500" "dt,labtype"
;;
"ads_backend_ec_error_reason_all_4500" )
    export_data "ads_backend_ec_error_reason_all_4500" "dt,reason"
;;
"ads_backend_ec_error_reason_both_4500" )
    export_data "ads_backend_ec_error_reason_both_4500" "dt,reason"
;;
"ads_backend_ea_error_reason_all_4500" )
    export_data "ads_backend_ea_error_reason_all_4500" "dt,reason"
;;
"ads_backend_ea_error_reason_both_4500" )
    export_data "ads_backend_ea_error_reason_both_4500" "dt,reason"
;;
"ads_licode_connection_stream_4500" )
    export_data "ads_licode_connection_stream_4500" "dt"
;;
"ads_serv_recover_total" )
	export_data "ads_serv_recover_total" "dt,serv"
;;
"ads_product_banner_total" )
	export_data "ads_product_banner_total" "dt,platform,banner,picture"
;;
"ads_mac_avg_total" )
	export_data "ads_mac_avg_total" "dt"
;;
"ads_serv_all_not_present_total" )
	export_data "ads_serv_all_not_present_total" "dt,relayservername,time_flag"
;;
"dwd_anti_fraud_data" )
	export_data_1 "dwd_anti_fraud_data" "ts,suid,duid,link_time"
;;
"dwd_anti_fraud_order" )
	export_data_1 "dwd_anti_fraud_order" "user_id,pay_time,phone"
;;
"ads_licode_sfu_total" )
	export_data "ads_licode_sfu_total" "dt,flag"
;;
"ads_mobile_vedio_total" )
	export_data "ads_mobile_vedio_total" "dt,platform,version,connect_type"
;;
"ads_mobile_conn_fst_time_total" )
	export_data "ads_mobile_conn_fst_time_total" "dt,platform,version,connect_type"
;;
"ads_mobile_dec_total" )
	export_data "ads_mobile_dec_total" "dt,decc"
;;
"ads_mobile_enc_total" )
	export_data "ads_mobile_enc_total" "dt,enc,codec_name,enc_err_code"
;;
"ads_mobile_rtc_cap_rate" )
	export_data "ads_mobile_rtc_cap_rate" "dt,platform,version,connect_type"
;;
"ads_mobile_rtc_tar_rate" )
	export_data "ads_mobile_rtc_tar_rate" "dt,platform,version,connect_type"
;;
"ads_mobile_rtc_rtt" )
	export_data "ads_mobile_rtc_rtt" "dt,platform,version,connect_type"
;;
"ads_mobile_rtc_loss" )
	export_data "ads_mobile_rtc_loss" "dt,platform,version,connect_type"
;;
"ads_su_country_total" )
	export_data "ads_su_country_total" "dt,province_name,customer_type"
;;
"ads_du_country_total" )
        export_data "ads_du_country_total" "dt,province_name,customer_type"
;;
"ads_bad_serv_top" )
	export_data "ads_bad_serv_top" "dt,serv"
;;
"ads_tob_bandwidth_total" )
	export_data "ads_tob_bandwidth_total" "dt,flag,id"
;;
"ads_vip_conn_duration" )
	export_data "ads_vip_conn_duration" "dt,vip"
;;
"tt1" )
	export_data "tt1" "flag,key1"
;;
"ads_process_session_day" )
	export_data "ads_process_session_day" "dt,labtype"
;;
"ads_client_activedu" )
	export_data "ads_client_activedu" "dt,client_type,active_days"
;;
"ads_client_activesu" )
	export_data "ads_client_activesu" "dt,client_type,active_days"
;;
"ads_operation_total" )
	export_data "ads_operation_total" "dt,act_flag,id"
;;
"ads_file_transport_total" )
	export_data "ads_file_transport_total" "dt,labtype"
;;
"ads_daas_serv_session_day" )
	export_data "ads_daas_serv_session_day" "dt,relay_server_name"
;;
"ads_daas_country_session_day" )
	export_data "ads_daas_country_session_day" "dt,scountry,dcountry"
;;
"ads_daas_sprovince_session_day" )
	export_data "ads_daas_sprovince_session_day" "dt,sprovince"
;;
"ads_daas_dprovince_session_day" )
	export_data "ads_daas_dprovince_session_day" "dt,dprovince"
;;
"ads_daas_handshake" )
	export_data "ads_daas_handshake" "dt,type"
;;
"ads_connect_limit_total" )
	export_data "ads_connect_limit_total" "dt,flag"
;;
"ads_daas_licode_handshake" )
        export_data "ads_daas_licode_handshake" "dt,type"
;;
"ads_tob_company_total" )
	export_data "ads_tob_company_total" "dt,flag,company_id"
;;
"ads_connect_active_total" )
	export_data "ads_connect_active_total" "dt,flag,client"
;;
"ads_user_limit_total" )
	export_data "ads_user_limit_total" "dt,reason"
;;
ads_platform_detail_total )
	export_data_2 "ads_platform_detail_total"
;;
ads_daas_client_session_day )
	export_data "ads_daas_client_session_day" "dt,sclient,is_trial"
;;
ads_daas_forward_failed_total )
	export_data "ads_daas_forward_failed_total" "dt,reason"
;;
ads_ec_total )
export_data "ads_ec_total" "dt,clientype,platform"
;;
ads_todesk_forward_failed_total )
export_data "ads_todesk_forward_failed_total" "dt,reconnect,type"
;;
ads_daas_order_total )
export_data "ads_daas_order_total" "dt,is_todesk,is_renewal,product_price_id"
;;
ads_daas_used_time_total )
export_data "ads_daas_used_time_total" "dt,product_price_id,flag"
;;
ads_daas_isp_handshake )
export_data "ads_daas_isp_handshake" "dt,sisp,disp"
;;
"lc" )
	export_data_2 "ads_platform_detail_total"
;;
"c" )
export_data "ads_session_day" "dt,enterprise,reconnect"
        export_data "ads_conn_duration" "dt,enterprise"
        export_data "ads_su_client_version_total" "dt,client_type,version"
        export_data "ads_du_client_version_total" "dt,client_type,version"
        export_data "ads_client_connect" "dt"
        export_data "ads_client_closed" "dt"
        export_data "ads_user_logout" "dt"
        export_data "ads_add_device" "dt"
        export_data "ads_delete_device" "dt"
        export_data "ads_edit_device" "dt"
        export_data "ads_add_group" "dt"
        export_data "ads_delete_group" "dt"
        export_data "ads_edit_group" "dt"
        export_data "ads_android_logic_total" "dt"
        export_data "ads_android_connect_total" "dt"
        export_data "ads_phone_new_user" "dt,platform"
        export_data "ads_android_su_action" "dt,action_type"
        export_data "ads_android_bus" "dt,bus"
        export_data "ads_android_resolution" "dt,resolution"
        export_data "ads_android_bios" "dt,platform,vendor,bios"
        export_data "ads_android_signin_total" "dt"
        export_data "ads_sclient_session" "dt,sclient_type"
        export_data "ads_phone_user_version" "dt,client_type,version"
        export_data "ads_bus_new_user" "dt,bus"
        export_data "ads_bus_user_start" "dt,bus"
        export_data "ads_new_user" "dt"
        export_data "ads_start_user" "dt,action_type"
        export_data "ads_client_new_mac" "dt,client_type"
        export_data "ads_client_start_mac" "dt,client_type,action_type"
        export_data "ads_client_new_su" "dt,client_type"
        export_data "ads_client_start_su" "dt,client_type,action_type"
        export_data "ads_client_new_du" "dt,client_type"
        export_data "ads_client_start_du" "dt,client_type,action_type"
        export_data "ads_bus_user_retention" "bus,action_type,create_time,retention_day"
        export_data "ads_user_retention" "action_type,create_time,retention_day"
        export_data_2 "ads_platform_detail_total"
export_data_2 "ads_platform_file_detail_total"
export_data_2 "ads_platform_vc_detail_total"
export_data "ads_sec_verif_total" "dt,mode,flag"
export_data "ads_cross_provincial_total" "dt,is_relay"
export_data "ads_limit_user_total" "dt,flag,userid"
export_data "ads_limit_tpy_total" "dt,duid"
;;
  "cc" )
export_data "ads_mac_retention" "client_type,action_type,create_time,retention_day"
        export_data "ads_su_retention" "client_type,action_type,create_time,retention_day"
        export_data "ads_du_retention" "client_type,action_type,create_time,retention_day"
        export_data "ads_version_mac_total" "dt,version"
        export_data "ads_su_handshake_total" "dt,version"
        export_data "ads_du_handshake_total" "dt,version"
        export_data "ads_su_handshake_failure" "dt,action,version,errno"
        export_data "ads_du_handshake_failure" "dt,action,version,errno"
        export_data "ads_user_login_total" "dt,login_type,enterprise"
        export_data "ads_ip_user_login_total" "dt,login_type,enterprise"
        export_data "ads_vendor_process_total" "dt,vendor"
        export_data "ads_windows_first_frame_total" "dt,version,forward,action"
        export_data "ads_android_action_total" "dt,platform,version,action"
        export_data "ads_android_action_pvuv" "dt,platform,version,action_type"
        export_data "ads_android_du_total" "dt,version"
        export_data "ads_android_setting_total" "dt,version,platform"
        export_data "ads_android_test_double" "dt,test_double_value"
        export_data "ads_client_version_retention" "first_su_date,client_type,retention_day"
        export_data "ads_client_version_new_su" "dt,client_type"
        export_data "ads_client_version_du_retention" "first_du_date,client_type,retention_day"
        export_data "ads_client_version_du_total" "dt,client_type"
        export_data "ads_rtc_version_conn_total" "dt,version,connect_type"
  export_data "ads_rtc_version_vedio_total" "dt,version,connect_type,local_proto"
  export_data "ads_rtc_version_fallback_total" "dt,version"
  export_data "ads_rtc_version_conn_fst_time" "dt,version,connect_type,local_proto"
  export_data "ads_rtc_version_cap_send" "dt,version,connect_type,local_proto"
  export_data "ads_rtc_version_rcvd_rend" "dt,version,connect_type,local_proto"
  export_data "ads_rtc_version_du_rate" "dt,version,connect_type,local_proto"
  export_data "ads_rtc_version_rtt" "dt,version,connect_type,local_proto"
  export_data "ads_rtc_version_loss" "dt,version,connect_type,local_proto"
  export_data "ads_android_login_total" "dt,login_type,version"
  export_data "ads_web_setting_total" "dt"
  export_data "ads_su_province_total" "dt,iso_code_3166,customer_type"
  export_data "ads_ios_licode_total" "dt,version,turndata_useLicode,licode_tyoe"
  export_data "ads_android_licode_total" "dt,version,turndata_useLicode,licode_tyoe"
  export_data "ads_licode_sdk_total" "dt,version"
 export_data "ads_dass_connect_fail_total" "dt,flag,id"
    ;;
  "ccc" )
        export_data "ads_licode_sig_total" "dt,sdk_version,url"
        export_data "ads_licode_ice_total" "dt,sdk_version,ice_connection_type"
        export_data "ads_windows_first_frame_fail" "dt,version,conn_type,failure_type"
        export_data "ads_windows_first_frame_success" "dt,version,conn_type,time_type"
        export_data "ads_licode_conn_time_total" "dt"
        export_data "ads_version_new_connect_total" "dt,labtype,client,version"
        export_data "ads_version_new_connect_clientype_total" "dt,clientype,version"
        export_data "ads_backend_licode_loss_total" "dt,type,metadata_type"
        export_data "ads_windows_clarity_total" "dt,type"
        export_data "ads_id_request_phone_total" "dt,type"
        export_data "ads_version_new_connect_type_total" "dt,du_type,dversion"
        export_data "ads_android_logic_topic_total" "dt,client_type,bus"
        export_data "ads_android_logic_retention" "first_logic_date,client_type,bus,retention_day"
        export_data "ads_p2p_network_total" "dt,client_network,host_network,client_version,host_version"
        export_data "ads_serv_diff_total" "dt,ads_serv_diff_total"
        export_data "ads_client_newuser_newsu" "dt,client_type"
        export_data "ads_client_newuser_suretention" "first_su_date,client_type,retention_day"
        export_data "ads_client_newuser_newdu" "dt,client_type"
        export_data "ads_client_newuser_duretention" "first_du_date,action_type,retention_day"
        export_data "ads_bigdata_datas_total" "dt,log_type"
        export_data "ads_pc_new_version_total" "dt,version"
export_data "ads_serv_recover_total" "dt,serv"
export_data "ads_product_banner_total" "dt,platform,banner,picture"
#export_data "ads_mac_avg_total" "dt"
#export_data "ads_serv_all_not_present_total" "dt,relayservername,time_flag"
#export_data_1 "dwd_anti_fraud_data" "ts,suid,duid,link_time"
#export_data_1 "dwd_anti_fraud_order" "user_id,pay_time,phone"
export_data "ads_licode_sfu_total" "dt,flag"
export_data "ads_mobile_vedio_total" "dt,platform,version,connect_type"
export_data "ads_mobile_conn_fst_time_total" "dt,platform,version,connect_type"
export_data "ads_mobile_dec_total" "dt,dec"
export_data "ads_mobile_enc_total" "dt,enc,codec_name,enc_err_code"
export_data "ads_mobile_rtc_cap_rate" "dt,platform,version,connect_type"
export_data "ads_mobile_rtc_tar_rate" "dt,platform,version,connect_type"
export_data "ads_mobile_rtc_rtt" "dt,platform,version,connect_type"
export_data "ads_mobile_rtc_loss" "dt,platform,version,connect_type"
export_data "ads_su_country_total" "dt,province_name,customer_type"
export_data "ads_du_country_total" "dt,province_name,customer_type"
export_data "ads_bad_serv_top" "dt,serv"
#export_data "ads_tob_bandwidth_total" "dt,flag,id"
export_data "ads_vip_conn_duration" "dt,vip"
export_data "ads_process_session_day" "dt,labtype,reconnect"
export_data "ads_client_activesu" "dt,client_type,active_days"
export_data "ads_client_activedu" "dt,client_type,active_days"
export_data "ads_file_transport_total" "dt,labtype"
export_data "ads_daas_serv_session_day" "dt,relay_server_name"
export_data "ads_daas_country_session_day" "dt,scountry,dcountry"
export_data "ads_daas_sprovince_session_day" "dt,sprovince"
export_data "ads_daas_dprovince_session_day" "dt,dprovince"
export_data "ads_daas_handshake" "dt,type"
export_data "ads_connect_limit_total" "dt,flag"
export_data "ads_daas_licode_handshake" "dt,type"
export_data "ads_tob_company_total" "dt,flag,company_id"
export_data "ads_connect_active_total" "dt,flag,client"
export_data "ads_user_limit_total" "dt,reason"
export_data "ads_daas_forward_failed_total" "dt,reason"
export_data "ads_daas_client_session_day" "dt,sclient,is_trial"
export_data "ads_ec_total" "dt,clientype,platform"
export_data "ads_todesk_forward_failed_total" "dt,reconnect,type"
export_data "ads_daas_order_total" "dt,is_todesk,is_renewal,product_price_id"
export_data "ads_daas_used_time_total" "dt,product_price_id,flag"
export_data "ads_daas_isp_handshake" "dt,sisp,disp"
export_data "ads_forward_hour_isp_total" "dt,client,hour,isp"
export_data "ads_ec_hour_isp_total" "dt,client,hour,isp"
export_data "ads_ea_hour_isp_total" "dt,client,hour,isp"
export_data "ads_windows_tfa_total" "dt,action,flag"
;;
  "all" )
        export_data "ads_session_day" "dt,enterprise,reconnect"
        export_data "ads_conn_duration" "dt,enterprise"
        export_data "ads_su_client_version_total" "dt,client_type,version"
        export_data "ads_du_client_version_total" "dt,client_type,version"
        export_data "ads_client_connect" "dt"
        export_data "ads_client_closed" "dt"
        export_data "ads_user_logout" "dt"
        export_data "ads_add_device" "dt"
        export_data "ads_delete_device" "dt"
        export_data "ads_edit_device" "dt"
        export_data "ads_add_group" "dt"
        export_data "ads_delete_group" "dt"
        export_data "ads_edit_group" "dt"
        export_data "ads_android_logic_total" "dt"
        export_data "ads_android_connect_total" "dt"
        export_data "ads_phone_new_user" "dt,platform"
        export_data "ads_android_su_action" "dt,action_type"
        export_data "ads_android_bus" "dt,bus"
        export_data "ads_android_resolution" "dt,resolution"
        export_data "ads_android_bios" "dt,platform,vendor,bios"
        export_data "ads_android_signin_total" "dt"
        export_data "ads_sclient_session" "dt,sclient_type"
        export_data "ads_phone_user_version" "dt,client_type,version"
        export_data "ads_bus_new_user" "dt,bus"
        export_data "ads_bus_user_start" "dt,bus"
        export_data "ads_new_user" "dt"
        export_data "ads_start_user" "dt,action_type"
        export_data "ads_client_new_mac" "dt,client_type"
        export_data "ads_client_start_mac" "dt,client_type,action_type"
        export_data "ads_client_new_su" "dt,client_type"
        export_data "ads_client_start_su" "dt,client_type,action_type"
        export_data "ads_client_new_du" "dt,client_type"
        export_data "ads_client_start_du" "dt,client_type,action_type"
        export_data "ads_bus_user_retention" "bus,action_type,create_time,retention_day"
        export_data "ads_user_retention" "action_type,create_time,retention_day"
        export_data "ads_mac_retention" "client_type,action_type,create_time,retention_day"
        export_data "ads_su_retention" "client_type,action_type,create_time,retention_day"
        export_data "ads_du_retention" "client_type,action_type,create_time,retention_day"
        export_data "ads_version_mac_total" "dt,version"
        export_data "ads_su_handshake_total" "dt,version"
        export_data "ads_du_handshake_total" "dt,version"
	export_data "ads_su_handshake_failure" "dt,action,version,errno"
  	export_data "ads_du_handshake_failure" "dt,action,version,errno"
	export_data "ads_user_login_total" "dt,login_type,enterprise"
	export_data "ads_ip_user_login_total" "dt,login_type,enterprise"
	export_data "ads_vendor_process_total" "dt,vendor"
	export_data "ads_windows_first_frame_total" "dt,version,forward,action"
	export_data "ads_android_action_total" "dt,platform,version,action"
        export_data "ads_android_action_pvuv" "dt,platform,version,action_type"
	export_data "ads_android_du_total" "dt,version"
        export_data "ads_android_setting_total" "dt,version,platform"
        export_data "ads_android_test_double" "dt,test_double_value"
	export_data "ads_client_version_retention" "first_su_date,client_type,retention_day"
	export_data "ads_client_version_new_su" "dt,client_type"
	export_data "ads_client_version_du_retention" "first_du_date,client_type,retention_day"
	export_data "ads_client_version_du_total" "dt,client_type"
	export_data "ads_rtc_version_conn_total" "dt,version,connect_type"
  export_data "ads_rtc_version_vedio_total" "dt,version,connect_type,local_proto"
  export_data "ads_rtc_version_fallback_total" "dt,version"
  export_data "ads_rtc_version_conn_fst_time" "dt,version,connect_type,local_proto"
  export_data "ads_rtc_version_cap_send" "dt,version,connect_type,local_proto"
  export_data "ads_rtc_version_rcvd_rend" "dt,version,connect_type,local_proto"
  export_data "ads_rtc_version_du_rate" "dt,version,connect_type,local_proto"
  export_data "ads_rtc_version_rtt" "dt,version,connect_type,local_proto"
  export_data "ads_rtc_version_loss" "dt,version,connect_type,local_proto"
  export_data "ads_android_login_total" "dt,login_type,version"
  export_data "ads_web_setting_total" "dt"
  export_data "ads_su_province_total" "dt,iso_code_3166,customer_type"
  export_data "ads_ios_licode_total" "dt,version,turndata_useLicode,licode_tyoe"
  export_data "ads_android_licode_total" "dt,version,turndata_useLicode,licode_tyoe"
  export_data "ads_licode_sdk_total" "dt,version"
        export_data "ads_licode_sig_total" "dt,sdk_version,url"
        export_data "ads_licode_ice_total" "dt,sdk_version,ice_connection_type"
	export_data "ads_windows_first_frame_fail" "dt,version,conn_type,failure_type"
        export_data "ads_windows_first_frame_success" "dt,version,conn_type,time_type"
	export_data "ads_licode_conn_time_total" "dt"
	export_data "ads_version_new_connect_total" "dt,labtype,client,version"
        export_data "ads_version_new_connect_clientype_total" "dt,clientype,version"
	export_data "ads_backend_licode_loss_total" "dt,type,metadata_type"
	export_data "ads_windows_clarity_total" "dt,type"
	export_data "ads_id_request_phone_total" "dt,type"
	export_data "ads_version_new_connect_type_total" "dt,du_type,dversion"
	export_data "ads_android_logic_topic_total" "dt,client_type,bus"
        export_data "ads_android_logic_retention" "first_logic_date,client_type,bus,retention_day"
        export_data "ads_p2p_network_total" "dt,client_network,host_network,client_version,host_version"
	export_data "ads_serv_diff_total" "dt,ads_serv_diff_total"
	export_data "ads_client_newuser_newsu" "dt,client_type"
	export_data "ads_client_newuser_suretention" "first_su_date,client_type,retention_day"
	export_data "ads_client_newuser_newdu" "dt,client_type"
	export_data "ads_client_newuser_duretention" "first_du_date,action_type,retention_day"
	export_data "ads_bigdata_datas_total" "dt,log_type"
	export_data "ads_pc_new_version_total" "dt,version"
export_data "ads_serv_recover_total" "dt,serv"
export_data "ads_product_banner_total" "dt,platform,banner,picture"
#export_data "ads_mac_avg_total" "dt"
#export_data "ads_serv_all_not_present_total" "dt,relayservername,time_flag"
#export_data_1 "dwd_anti_fraud_data" "ts,suid,duid,link_time"
#export_data_1 "dwd_anti_fraud_order" "user_id,pay_time,phone"
export_data "ads_licode_sfu_total" "dt,flag"
export_data "ads_mobile_vedio_total" "dt,platform,version,connect_type"
export_data "ads_mobile_conn_fst_time_total" "dt,platform,version,connect_type"
export_data "ads_mobile_dec_total" "dt,dec"
export_data "ads_mobile_enc_total" "dt,enc,codec_name,enc_err_code"
export_data "ads_mobile_rtc_cap_rate" "dt,platform,version,connect_type"
export_data "ads_mobile_rtc_tar_rate" "dt,platform,version,connect_type"
export_data "ads_mobile_rtc_rtt" "dt,platform,version,connect_type"
export_data "ads_mobile_rtc_loss" "dt,platform,version,connect_type"
export_data "ads_su_country_total" "dt,province_name,customer_type"
export_data "ads_du_country_total" "dt,province_name,customer_type"
export_data "ads_bad_serv_top" "dt,serv"
#export_data "ads_tob_bandwidth_total" "dt,flag,id"
export_data "ads_vip_conn_duration" "dt,vip"
export_data "ads_process_session_day" "dt,labtype,reconnect"
export_data "ads_client_activesu" "dt,client_type,active_days"
export_data "ads_client_activedu" "dt,client_type,active_days"
export_data "ads_file_transport_total" "dt,labtype"
export_data "ads_daas_serv_session_day" "dt,relay_server_name"
export_data "ads_daas_country_session_day" "dt,scountry,dcountry"
export_data "ads_daas_sprovince_session_day" "dt,sprovince"
export_data "ads_daas_dprovince_session_day" "dt,dprovince"
export_data "ads_daas_handshake" "dt,type"
export_data "ads_connect_limit_total" "dt,flag"
export_data "ads_daas_licode_handshake" "dt,type"
export_data "ads_tob_company_total" "dt,flag,company_id"
export_data "ads_connect_active_total" "dt,flag,client"
export_data "ads_user_limit_total" "dt,reason"
export_data "ads_daas_forward_failed_total" "dt,reason"
export_data "ads_daas_client_session_day" "dt,sclient,is_trial"
export_data "ads_ec_total" "dt,clientype,platform"
export_data "ads_todesk_forward_failed_total" "dt,reconnect,type"
export_data "ads_daas_order_total" "dt,is_todesk,is_renewal,product_price_id"
export_data "ads_daas_used_time_total" "dt,product_price_id,flag"
export_data "ads_daas_isp_handshake" "dt,sisp,disp"
export_data_2 "ads_platform_detail_total"
export_data_2 "ads_platform_file_detail_total"
export_data_2 "ads_platform_vc_detail_total"
export_data "ads_forward_hour_isp_total" "dt,client,hour,isp"
export_data "ads_ec_hour_isp_total" "dt,client,hour,isp"
export_data "ads_ea_hour_isp_total" "dt,client,hour,isp"
export_data "ads_windows_tfa_total" "dt,action,flag"
export_data "ads_sec_verif_total" "dt,mode,flag"
  ;;
esac

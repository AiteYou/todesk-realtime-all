#!/usr/bin/python
# coding: utf-8

import os
import shutil
from datetime import datetime
import subprocess

def move_directories_to_local(hdfs_paths, target_date, local_dir):
    target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
    for hdfs_path in hdfs_paths:
            war = hdfs_path.split('_',-1)[0]
            dst_path=local_dir+'/'+war+'/'+hdfs_path
            if not os.path.exists(dst_path):
                os.makedirs(dst_path)
            command = "hdfs dfs -ls /warehouse/todesk/'%s'/'%s'" %(war,hdfs_path)
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, error = process.communicate()
            lines = output.strip().split("\n")
            for line in lines:
                parts = line.split()
                if len(parts) >= 8:
                    dir_name = parts[-1]
                    try:
                        dir_date = datetime.strptime(dir_name.split("=")[1], "%Y-%m-%d").date()
                    except (IndexError, ValueError):
                        continue
                    if dir_date <= target_date:
                        src_path = os.path.join(hdfs_path, dir_name)
                        command = 'hdfs dfs -get "%s"  "%s"' % (src_path,dst_path)
                        subprocess.call(command, shell=True)
			print ('Moved directory "%s"  "%s"' % (src_path,dst_path))
def delete_hdfs_data(hdfs_paths, target_date):
    target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
    for hdfs_path in hdfs_paths:
        war = hdfs_path.split('_',-1)[0]
        command = "hadoop fs -rm -r -skipTrash /warehouse/todesk/'%s'/'%s'/dt=2025-01*" %(war,hdfs_path)
        subprocess.call(command, shell=True)
#永久dwd_phone_login,dwd_qrcode_login,dwd_wechat_login,dwd_new_connect,dwd_authentication_success,dwd_handshake_success,dwd_session_success,dwd_user_login,dwd_user_logout
#hdfs_paths = ["ods_mac_info","ods_order_buy","ods_tv_bill_order","ods_tv_bill_order_detail","ods_tv_function_config","ods_tv_try","dim_du_info","dim_mac_info","dim_su_info","dim_user_info","dws_backend_ea_failed_total_4500","dws_backend_ec_error_total_4500","dws_backend_licode_day_total","dws_backend_licode_day_total_4500","dws_backend_licode_ea_total_4500","dws_banversion_detail_total","dws_banversion_total","dws_bus_user_action","dws_client_session_day","dws_client_version_licode_day_total","dws_country_vip_total","dws_du_action","dws_du_action_daytotal","dws_hour_total","dws_ip_licode_day_total","dws_mac_action","dws_presenter_lost_total","dws_province_customer_day_total","dws_province_isp_client_day_total","dws_serv_connection_failure","dws_serv_total","dws_su_action","dws_su_action_daytotal","dws_user_action","dwt_android_topic","dwt_bus_user_topic","dwt_du_new_total_topic","dwt_du_topic","dwt_du_total_topic","dwt_mac_topic","dwt_su_new_total_topic","dwt_su_topic","dwt_su_total_topic","dwt_user_topic","dwd_add_device","dwd_add_group","dwd_android_muses","dwd_anti_fraud_data","dwd_anti_fraud_detail","dwd_anti_fraud_order","dwd_auto_login","dwd_backend_banversion","dwd_backend_licode_addPublisher_ready","dwd_backend_licode_addsubscriber_ready","dwd_backend_licode_all_ready","dwd_backend_licode_newconnect","dwd_backend_licode_publish_end","dwd_backend_licode_publish_erizo","dwd_backend_licode_publish_initializing","dwd_backend_licode_publish_ready","dwd_backend_licode_publish_start","dwd_backend_licode_reconnect","dwd_backend_licode_sdk_report","dwd_backend_licode_socket_disconnected","dwd_backend_licode_subscribe_end","dwd_backend_licode_subscribe_p2p","dwd_backend_licode_subscribe_ready","dwd_backend_licode_subscribe_sfu","dwd_backend_licode_subscribe_start","dwd_backend_licode_unpublish","dwd_backend_licode_unsubscribe","dwd_client_click","dwd_client_close_fake","dwd_client_closed","dwd_client_event","dwd_client_pageview","dwd_client_section_du","dwd_client_section_su","dwd_client_tevent","dwd_client_tuevent","dwd_connection_closed","dwd_connection_failure","dwd_controller_TCP_packet_failure","dwd_controller_lost","dwd_delete_device","dwd_delete_group","dwd_edit_device","dwd_edit_group","dwd_handshake_sd","dwd_licode_connect_rtc","dwd_licode_first_packet_receive","dwd_licode_ice","dwd_licode_ice_connection","dwd_licode_sdk","dwd_licode_session_success","dwd_licode_sig","dwd_login_error","dwd_media_statistic","dwd_new_connect_fail","dwd_new_user","dwd_order_buy","dwd_plugin_check","dwd_presenter_TCP_packet_failure","dwd_presenter_lost","dwd_rtc_android_du","dwd_rtc_android_su","dwd_sdk_du","dwd_sdk_su","dwd_tmp_duid","dwd_tv_bill_order_detail","dwd_tv_function_config","dwd_tv_macine","dwd_tv_package","dwd_tv_try","dwd_web_config_center","dwd_web_request"]
#hdfs_paths = ["ods_click_log","ods_client_log","ods_event_log","ods_now_log","ods_other_log","ods_tevent_log","ods_tuevent_log"]
hdfs_paths = ["ods_other_log","ods_tevent_log","ods_tuevent_log"]
#hdfs_paths = ["ods_click_log","ods_client_log","ods_event_log","ods_now_log"]
target_date = '2024-09-30'
local_dir = '/bigdata/data1'
move_directories_to_local(hdfs_paths, target_date, local_dir)
#delete_hdfs_data(hdfs_paths, target_date)

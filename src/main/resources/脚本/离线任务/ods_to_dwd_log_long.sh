#!/bin/bash

APP=todesk
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

dwd_handshake_sd="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_handshake_sd partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.type'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.serv'),
    get_json_object(line,'$.user'),
    get_json_object(line,'$.vip'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.client'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.sport'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'handshake';"

dwd_new_connect_fail="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_new_connect_fail partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.srcport'),
    get_json_object(line,'$.srcplatform'),
    get_json_object(line,'$.srcversion'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.source'),
    get_json_object(line,'$.runtime'),
    get_json_object(line,'$.resultmsg'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'newconnect_fail'
and get_json_object(line,'$.labtype') = 'logic';"

dwd_session_success="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_session_success partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.serv'),
    get_json_object(line,'$.linktime'),
    get_json_object(line,'$.in'),
    get_json_object(line,'$.out'),
    get_json_object(line,'$.closetype'),
    get_json_object(line,'$.user'),
    get_json_object(line,'$.vip'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.client'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.sport'),
    get_json_object(line,'$.duser'),
    get_json_object(line,'$.dvip'),
    get_json_object(line,'$.dversion'),
    get_json_object(line,'$.dclient'),
    get_json_object(line,'$.dip'),
    get_json_object(line,'$.dport'),
    get_json_object(line,'$.max'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'connection_stat';"

dwd_authentication_success="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_authentication_success partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.sport'),
    get_json_object(line,'$.client'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.user'),
    get_json_object(line,'$.vip'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.serv'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'authentication_success';"

dwd_user_logout="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_user_logout partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.sport'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.result'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'user_logout'
and get_json_object(line,'$.labtype') = 'logic'
and get_json_object(line,'$.suid') is not null;"

dwd_new_connect="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_new_connect partition (dt='$do_date')
select
    if(get_json_object(line,'$.suid') is not null,get_json_object(line,'$.suid'),get_json_object(line,'$.srcid')) ,
    if(get_json_object(line,'$.sip') is not null,get_json_object(line,'$.sip'), get_json_object(line,'$.srcip')),
    get_json_object(line,'$.srcport'),
    get_json_object(line,'$.srcplatform'),
    get_json_object(line,'$.srcversion'),
    get_json_object(line,'$.srcuser'),
    get_json_object(line,'$.srcvip'),
    get_json_object(line,'$.srcnew'),
    get_json_object(line,'$.srcline'),
    get_json_object(line,'$.srcisp'),
    get_json_object(line,'$.srccountry'),
    get_json_object(line,'$.srcprovince'),
    get_json_object(line,'$.srccity'),
    if(get_json_object(line,'$.duid') is not null,get_json_object(line,'$.duid'),get_json_object(line,'$.dstid')) ,
    if(get_json_object(line,'$.dip') is not null,get_json_object(line,'$.dip'),get_json_object(line,'$.dstip')) ,
    get_json_object(line,'$.dstport'),
    get_json_object(line,'$.dstplatform'),
    get_json_object(line,'$.dstversion'),
    get_json_object(line,'$.dstuser'),
    get_json_object(line,'$.dstvip'),
    get_json_object(line,'$.dstnew'),
    get_json_object(line,'$.dstline'),
    get_json_object(line,'$.dstisp'),
    get_json_object(line,'$.dstcountry'),
    get_json_object(line,'$.dstprovince'),
    get_json_object(line,'$.dstcity'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.action'),
    get_json_object(line,'$.relayserver'),
    get_json_object(line,'$.relayservername'),
    get_json_object(line,'$.sab'),
    get_json_object(line,'$.reconnect'),
    get_json_object(line,'$.idmax'),
    get_json_object(line,'$.accmac'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.bemail'),
    get_json_object(line,'$.ingroup'),
    get_json_object(line,'$.black'),
    get_json_object(line,'$.regdays'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.runtime'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'newconnect'
and get_json_object(line,'$.labtype') = 'logic'
and get_json_object(line,'$.suid') is not null
and get_json_object(line,'$.relayserver') is not null;"

dwd_handshake_success="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_handshake_success partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.sport'),
    get_json_object(line,'$.client'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.user'),
    get_json_object(line,'$.vip'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.dip'),
    get_json_object(line,'$.dport'),
    get_json_object(line,'$.dclient'),
    get_json_object(line,'$.dversion'),
    get_json_object(line,'$.duser'),
    get_json_object(line,'$.dvip'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.serv'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.in'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'handshake_success';"

dwd_controller_lost="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_controller_lost partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.serv'),
    get_json_object(line,'$.dclient'),
    get_json_object(line,'$.dversion'),
    get_json_object(line,'$.client'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'controller_not_present';"

dwd_presenter_lost="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_presenter_lost partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.serv'),
    get_json_object(line,'$.dclient'),
    get_json_object(line,'$.dversion'),
    get_json_object(line,'$.dip'),
    get_json_object(line,'$.client'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'presenter_not_present';"

dwd_add_device="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_add_device partition (dt='$do_date')
select
    get_json_object(line,'$.id'),
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.userid'),
    get_json_object(line,'$.groupid'),
    get_json_object(line,'$.result'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'add_device'
and get_json_object(line,'$.labtype')= 'logic';"

dwd_delete_device="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_delete_device partition (dt='$do_date')
select
    get_json_object(line,'$.id'),
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.userid'),
    get_json_object(line,'$.groupid'),
    get_json_object(line,'$.result'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'delete_device'
and get_json_object(line,'$.labtype')= 'logic';"

dwd_edit_device="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_edit_device partition (dt='$do_date')
select
    get_json_object(line,'$.id'),
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.userid'),
    get_json_object(line,'$.groupid'),
    get_json_object(line,'$.result'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'edit_device'
and get_json_object(line,'$.labtype')= 'logic';"


dwd_add_group="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_add_group partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.userid'),
    get_json_object(line,'$.groupid'),
    get_json_object(line,'$.groupname'),
    get_json_object(line,'$.result'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'add_group'
and get_json_object(line,'$.labtype')= 'logic';"

dwd_delete_group="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_delete_group partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.userid'),
    get_json_object(line,'$.groupid'),
    get_json_object(line,'$.groupname'),
    get_json_object(line,'$.result'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'delete_group'
and get_json_object(line,'$.labtype')= 'logic';"

dwd_edit_group="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_edit_group partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.userid'),
    get_json_object(line,'$.groupid'),
    get_json_object(line,'$.groupname'),
    get_json_object(line,'$.result'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'edit_group'
and get_json_object(line,'$.labtype')= 'logic';"


dwd_controller_TCP_packet_failure="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_controller_TCP_packet_failure partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.serv'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.sport'),
    get_json_object(line,'$.client'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='controller_TCP_packet_failure';"

dwd_presenter_TCP_packet_failure="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_presenter_TCP_packet_failure partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.serv'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.sport'),
    get_json_object(line,'$.client'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='presenter_TCP_packet_failure';"

dwd_new_user="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_new_user partition (dt='$do_date')
select
    get_json_object(line,'$.isphone'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.tdid'),
    get_json_object(line,'$.user'),
    get_json_object(line,'$.newreg'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='newuser';"

dwd_user_login="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_user_login partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.srcuser'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.vip'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.sport'),
    get_json_object(line,'$.dstuser'),
    get_json_object(line,'$.city'),
    get_json_object(line,'$.client'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.in'),
    get_json_object(line,'$.out'),
    get_json_object(line,'$.result'),
    get_json_object(line,'$.account'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='user_login';"

dwd_wechat_login="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_wechat_login partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.code'),
    get_json_object(line,'$.os'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.service'),
    get_json_object(line,'$.state'),
    get_json_object(line,'$.category'),
    get_json_object(line,'$.url'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='wechatLogin';"

dwd_phone_login="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_phone_login partition (dt='$do_date')
select
    get_json_object(line,'$.currentPhone'),
    get_json_object(line,'$.code'),
    get_json_object(line,'$.phone'),
    get_json_object(line,'$.service'),
    get_json_object(line,'$.category'),
    get_json_object(line,'$.url'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='phoneLogin';"

dwd_qrcode_login="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_qrcode_login partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.phone'),
    get_json_object(line,'$.service'),
    get_json_object(line,'$.category'),
    get_json_object(line,'$.email'),
    get_json_object(line,'$.token'),
    get_json_object(line,'$.url'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='qrcodeLogin';"

dwd_auto_login="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_auto_login partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.phone'),
    get_json_object(line,'$.service'),
    get_json_object(line,'$.category'),
    get_json_object(line,'$.passtoken'),
    get_json_object(line,'$.url'),
    get_json_object(line,'$.logintype'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='autoLogin';"

dwd_login_error="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_login_error partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.code'),
    get_json_object(line,'$.err'),
    get_json_object(line,'$.responseTime'),
    get_json_object(line,'$.query'),
    get_json_object(line,'$.params'),
    get_json_object(line,'$.url'),
    get_json_object(line,'$.ck_url'),
    get_json_object(line,'$.passbody'),
    get_json_object(line,'$.service'),
    get_json_object(line,'$.category'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='errorlog';"

dwd_connection_failure="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_connection_failure partition (dt='$do_date')
select
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.logtype'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') in ('presenter_not_present','controller_not_present','all_not_present','controller_TCP_packet_failure','presenter_TCP_packet_failure');"



dwd_backend_licode_addsubscriber_ready="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_addsubscriber_ready partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.clientid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='addSubscriber_ready';"

dwd_backend_licode_addPublisher_ready="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_addPublisher_ready partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.clientid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='addPublisher_ready';"

dwd_backend_licode_all_ready="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_all_ready partition (dt='$do_date')
select
    t1.ip,
    t1.connid,
    t1.streamid,
    t1.logtime
from
(
    select
        streamid,
        connid, 
        ip,
        logtime
    from ${APP}.dwd_backend_licode_addPublisher_ready
    where dt ='$do_date'
    and streamid is not null
    group by streamid, connid, logtime ,ip
)t1
left join 
(
    select
        streamid
    from ${APP}.dwd_backend_licode_addsubscriber_ready
    where dt ='$do_date'
    and streamid is not null
    group by streamid
)t2
on t1.streamid=t2.streamid
where t2.streamid is not null;"

dwd_backend_licode_publish_erizo="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_publish_erizo partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.timestamp'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='publish_erizo';"

dwd_backend_licode_publish_initializing="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_publish_initializing partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='publish_initializing';"

dwd_backend_licode_subscribe_start="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_subscribe_start partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.timestamp'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='subscribe_start';"

dwd_backend_licode_subscribe_p2p="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_subscribe_p2p partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='subscribe_p2p';"



dwd_connection_closed="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_connection_closed partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.serv'),
    get_json_object(line,'$.type'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='connection_closed';"

dwd_licode_session_success="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_licode_session_success partition (dt='$do_date')
select
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.in'),
    get_json_object(line,'$.out'),
    get_json_object(line,'$.isP2P'),
    get_json_object(line,'$.linktime'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='licode_connection_stat';"

dwd_media_statistic="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_media_statistic partition (dt='$do_date')
select
    get_json_object(line,'$.streamId'),
    get_json_object(get_json_object(line,'$.message'),'$.isWrap'),
    get_json_object(get_json_object(line,'$.message'),'$.metadata-type'),
    get_json_object(get_json_object(line,'$.message'),'$.maxSeqNum'),
    get_json_object(get_json_object(line,'$.message'),'$.minSeqNum'),
    get_json_object(get_json_object(line,'$.message'),'$.videoPacketCount'),
    get_json_object(get_json_object(line,'$.message'),'$.delay0_10_ms'),
    get_json_object(get_json_object(line,'$.message'),'$.delay10_50_ms'),
    get_json_object(get_json_object(line,'$.message'),'$.delay100_1000_ms'),
    get_json_object(get_json_object(line,'$.message'),'$.delay1000_ms'),
    get_json_object(get_json_object(line,'$.message'),'$.duration0_10_ms'),
    get_json_object(get_json_object(line,'$.message'),'$.duration10_50_ms'),
    get_json_object(get_json_object(line,'$.message'),'$.duration100_1000_ms'),
    get_json_object(get_json_object(line,'$.message'),'$.duration1000_ms'),
    case
        when get_json_object(get_json_object(line,'$.message'),'$.isWrap')='1' then cast(((65535-get_json_object(get_json_object(line,'$.message'),'$.minSeqNum')+get_json_object(get_json_object(line,'$.message'),'$.maxSeqNum'))-get_json_object(get_json_object(line,'$.message'),'$.videoPacketCount'))/(65535-get_json_object(get_json_object(line,'$.message'),'$.minSeqNum')+get_json_object(get_json_object(line,'$.message'),'$.maxSeqNum')) *100 as DECIMAL(16,2))
        when get_json_object(get_json_object(line,'$.message'),'$.isWrap')='0' then cast((get_json_object(get_json_object(line,'$.message'),'$.maxSeqNum')-get_json_object(get_json_object(line,'$.message'),'$.minSeqNum')-get_json_object(get_json_object(line,'$.message'),'$.videoPacketCount'))/(get_json_object(get_json_object(line,'$.message'),'$.maxSeqNum')-get_json_object(get_json_object(line,'$.message'),'$.minSeqNum')) *100 as DECIMAL(16,2))
    end ,
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='media_statistic'
and get_json_object(line,'$.labtype')='ea';"

dwd_web_request="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_web_request partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.ck_url'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='request'
and get_json_object(line,'$.suid') is not null;"


dwd_client_pageview="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_client_pageview partition (dt='$do_date')
select
    get_json_object(line,'$.id'),
    get_json_object(line,'$.accountid'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.module'),
    get_json_object(line,'$.page'),
    get_json_object(line,'$.clienttime'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') ='pageview';"


dwd_backend_licode_newconnect="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_newconnect partition (dt='$do_date')
select
    id, role,
    case when t2.role='viewer' then t3.sclient
         when t2.role='presenter' then t3.dclient
         else '' end ,
    t2.connid, ip, isRelay,ts, logtime
from
(
    select
        get_json_object(line,'$.src') id,
        get_json_object(line,'$.role') role,
        get_json_object(line,'$.connId') connid,
        get_json_object(line,'$.ip') ip,
        get_json_object(line,'$.isRelay') isRelay,
	get_json_object(line,'$.timestamp') ts,
        get_json_object(line,'$.logtime') logtime
    from ${APP}.ods_now_log
    where dt ='$do_date'
    and get_json_object(line,'$.logtype')='licode_newconnection'
)t2
left join
(
    select
        connid,sclient,dclient
    from ${APP}.dwd_new_connect
    where dt='$do_date'
)t3
on t2.connid=t3.connid;"

dwd_backend_licode_reconnect="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_reconnect partition (dt='$do_date')
select
    get_json_object(line,'$.src'),
    get_json_object(line,'$.role'),
    get_json_object(line,'$.connId'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.isRelay'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='licode_reconnection';"

dwd_backend_licode_sdk_report="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_sdk_report partition (dt='$do_date')
select
    get_json_object(line,'$.connId'),
    get_json_object(line,'$.streamId'),
    get_json_object(line,'$.role'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.unpublish_reason'),
    get_json_object(line,'$.logtime'),
    get_json_object(line,'$.timestamp')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'sdk_report';"

dwd_backend_licode_socket_disconnected="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_socket_disconnected partition (dt='$do_date')
select
    get_json_object(line,'$.connId'),
    get_json_object(line,'$.role'),
    get_json_object(line,'$.src'),
    get_json_object(line,'$.isRelay'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.reason'),
    get_json_object(line,'$.logtime'),
    get_json_object(line,'$.timestamp')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'socket_disconnected';"

dwd_backend_licode_unpublish="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_unpublish partition (dt='$do_date')
select
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.isrelay'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.roomid'),
    get_json_object(line,'$.logtime'),
    get_json_object(line,'$.timestamp')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'onUnpublish';"

dwd_backend_licode_unsubscribe="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_unsubscribe partition (dt='$do_date')
select
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.isrelay'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.roomid'),
    get_json_object(line,'$.logtime'),
    get_json_object(line,'$.timestamp')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'onUnsubscribe';"

dwd_backend_licode_subscribe_end="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_subscribe_end partition (dt='$do_date')
select
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.roomid'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'subscribe_end';"

dwd_backend_licode_publish_end="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_publish_end partition (dt='$do_date')
select
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.roomid'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'publish_end';"

dwd_client_tuevent="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_client_tuevent partition (dt='$do_date')
select
    get_json_object(line,'$.bus'),
    get_json_object(line,'$.clientype'),
    get_json_object(line,'$.memory'),
    get_json_object(line,'$.bios'),
    get_json_object(line,'$.city'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.tz'),
    get_json_object(line,'$.language'),
    get_json_object(line,'$.resolution'),
    get_json_object(line,'$.mac'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.network'),
    get_json_object(line,'$.accountid'),
    get_json_object(line,'$.province'),
    get_json_object(line,'$.vendor'),
    get_json_object(line,'$.action'),
    if(get_json_object(line,'$.events') in('link_info','section') and (get_json_object(line,'$.version') not between '1.0.2.0' and '1.2.0.0' ),'',get_json_object(line,'$.turndata')),
    get_json_object(get_json_object(line,'$.turndata'),'$.result_code'),
    get_json_object(get_json_object(line,'$.turndata'),'$.test_double'),
    get_json_object(get_json_object(line,'$.turndata'),'$.connectId'),
    get_json_object(get_json_object(line,'$.turndata'),'$.useLicode'),
    get_json_object(get_json_object(line,'$.turndata'),'$.process'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.id'),
    get_json_object(line,'$.clienttime'),
    get_json_object(line,'$.events'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.cpu'),
    get_json_object(line,'$.gpu'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.pcid'),
    get_json_object(line,'$.mainboard'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_tuevent_log
where dt ='$do_date' 
and ((get_json_object(line,'$.events')='rtc_link_tech' and get_json_object(line,'$.action')='link_process' and get_json_object(line,'$.mainboard')='todesk-daas') or get_json_object(line,'$.events') in ('link_info','section','link_tech','setting_tech','remote_state_process'))
and get_json_object(line,'$.action') <> '34';"



dwd_sdk_du="
set hive.support.quoted.identifiers=None;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
with
rtc_su as
(
    select
        line
    from ${APP}.ods_tuevent_log
    where dt = '$do_date'
    and get_json_object(line, '$.events') = 'link_info'
    and get_json_object(line, '$.clientype') = 'client'
    and get_json_object(line, '$.connectionid') <> ''
),
rtc_du as
(
    select
        line
    from ${APP}.ods_tuevent_log
    where dt = '$do_date'
    and get_json_object(line, '$.events') = 'link_info'
    and get_json_object(line, '$.clientype') = 'host'
    and get_json_object(line, '$.connectionid') <> ''
),
rtc_etl as
(
    select 
        nvl(t1.connectionid,t2.connectionid) connid,t1.roll su_roll,t2.roll du_roll,
        nvl(t1.ConnTime,t2.ConnTime) ConnTime,
        nvl(t1.ConnState,t2.ConnState) ConnState,
        nvl(t1.ConnType,t2.ConnType) ConnType,
        nvl(t1.GatherState,t2.GatherState) GatherState,
        nvl(t1.SigState,t2.SigState) SigState,
        nvl(t1.IceState,t2.IceState) IceState,
        nvl(t1.StreamId,t2.StreamId) StreamId,
        nvl(t1.IsRTC,t2.IsRTC) IsRTC,
        nvl(t1.EBytes,t2.EBytes) EBytes,
        nvl(t1.HBytes,t2.HBytes) HBytes,
        nvl(t1.PBytes,t2.PBytes) PBytes,
        nvl(t1.RBytes,t2.RBytes) RBytes,
        nvl(t1.Enc,t2.Enc) Enc,
        nvl(t1.CodecName,t2.CodecName) CodecName,
        nvl(t1.LocalIpport,t2.LocalIpport) LocalIpport,
        nvl(t1.LocalProto,t2.LocalProto) LocalProto,
        nvl(t1.LocalType,t2.LocalType) LocalType,
        nvl(t1.RemotePort,t2.RemotePort) RemotePort,
        nvl(t1.RemoteProto,t2.RemoteProto) RemoteProto,
        nvl(t1.RemoteType,t2.RemoteType) RemoteType,
        nvl(t1.CpuMax,t2.CpuMax) CpuMax,
        nvl(t1.CpuMin,t2.CpuMin) CpuMin,
        nvl(t1.CpuMean,t2.CpuMean) CpuMean,
        nvl(t1.CpuStd,t2.CpuStd) CpuStd,
        nvl(t1.CapFrMax,t2.CapFrMax) CapFrMax,
        nvl(t1.CapFrMin,t2.CapFrMin) CapFrMin,
        nvl(t1.CapFrMean,t2.CapFrMean) CapFrMean,
        nvl(t1.CapFrStd,t2.CapFrStd) CapFrStd,
        nvl(t1.QpMax,t2.QpMax) QpMax,
        nvl(t1.QpMin,t2.QpMin) QpMin,
        nvl(t1.QpMean,t2.QpMean) QpMean,
        nvl(t1.QpStd,t2.QpStd) QpStd,
        nvl(t1.SendFrMax,t2.SendFrMax) SendFrMax,
        nvl(t1.SendFrMin,t2.SendFrMin) SendFrMin,
        nvl(t1.SendFrMean,t2.SendFrMean) SendFrMean,
        nvl(t1.SendFrStd,t2.SendFrStd) SendFrStd,
        nvl(t1.TarRateMax,t2.TarRateMax) TarRateMax,
        nvl(t1.TarRateMin,t2.TarRateMin) TarRateMin,
        nvl(t1.TarRateMean,t2.TarRateMean) TarRateMean,
        nvl(t1.TarRateStd,t2.TarRateStd) TarRateStd,
        nvl(t1.ActRateMax,t2.ActRateMax) ActRateMax,
        nvl(t1.ActRateMin,t2.ActRateMin) ActRateMin,
        nvl(t1.ActRateMean,t2.ActRateMean) ActRateMean,
        nvl(t1.ActRateStd,t2.ActRateStd) ActRateStd,
        nvl(t1.RttMax,t2.RttMax) RttMax,
        nvl(t1.RttMin,t2.RttMin) RttMin,
        nvl(t1.RttMean,t2.RttMean) RttMean,
        nvl(t1.RttStd,t2.RttStd) RttStd,
        nvl(t1.LossMax,t2.LossMax) LossMax,
        nvl(t1.LossMin,t2.LossMin) LossMin,
        nvl(t1.LossMean,t2.LossMean) LossMean,
        nvl(t1.LossStd,t2.LossStd) LossStd,
        nvl(t1.RlLossMax,t2.RlLossMax) RlLossMax,
        nvl(t1.RlLossMin,t2.RlLossMin) RlLossMin,
        nvl(t1.RlLossMean,t2.RlLossMean) RlLossMean,
        nvl(t1.RlLossStd,t2.RlLossStd) RlLossStd,
        nvl(t1.SendRateMax,t2.SendRateMax) SendRateMax,
        nvl(t1.SendRateMin,t2.SendRateMin) SendRateMin,
        nvl(t1.SendRateMean,t2.SendRateMean) SendRateMean,
        nvl(t1.SendRateStd,t2.SendRateStd) SendRateStd
    from
    (
        select
            get_json_object(line, '$.connectionid') connectionid,
            get_json_object(get_json_object(line,'$.turndata'),'$.rollback') roll,
            get_json_object(get_json_object(line,'turndata'), '$.ConnTime') ConnTime,
            get_json_object(get_json_object(line,'turndata'), '$.ConnState') ConnState,
            get_json_object(get_json_object(line,'turndata'), '$.ConnType') ConnType,
            get_json_object(get_json_object(line,'turndata'), '$.GatherState') GatherState,
            get_json_object(get_json_object(line,'turndata'), '$.SigState') SigState,
            get_json_object(get_json_object(line,'turndata'), '$.IceState') IceState,
            get_json_object(get_json_object(line,'turndata'), '$.StreamId') StreamId,
            get_json_object(get_json_object(line,'turndata'), '$.IsRTC') IsRTC,
            get_json_object(get_json_object(line,'turndata'), '$.EBytes') EBytes,
            get_json_object(get_json_object(line,'turndata'), '$.HBytes') HBytes,
            get_json_object(get_json_object(line,'turndata'), '$.PBytes') PBytes,
            get_json_object(get_json_object(line,'turndata'), '$.RBytes') RBytes,
            get_json_object(get_json_object(line,'turndata'), '$.Enc') Enc,
            get_json_object(get_json_object(line,'turndata'), '$.CodecName') CodecName,
            get_json_object(get_json_object(line,'turndata'), '$.LocalIpport') LocalIpport,
            get_json_object(get_json_object(line,'turndata'), '$.LocalProto') LocalProto,
            get_json_object(get_json_object(line,'turndata'), '$.LocalType') LocalType,
            get_json_object(get_json_object(line,'turndata'), '$.RemotePort') RemotePort,
            get_json_object(get_json_object(line,'turndata'), '$.RemoteProto') RemoteProto,
            get_json_object(get_json_object(line,'turndata'), '$.RemoteType') RemoteType,
            get_json_object(get_json_object(line,'turndata'), '$.CpuMax') CpuMax,
            get_json_object(get_json_object(line,'turndata'), '$.CpuMin') CpuMin,
            get_json_object(get_json_object(line,'turndata'), '$.CpuMean') CpuMean,
            get_json_object(get_json_object(line,'turndata'), '$.CpuStd') CpuStd,
            get_json_object(get_json_object(line,'turndata'), '$.CapFrMax') CapFrMax,
            get_json_object(get_json_object(line,'turndata'), '$.CapFrMin') CapFrMin,
            get_json_object(get_json_object(line,'turndata'), '$.CapFrMean') CapFrMean,
            get_json_object(get_json_object(line,'turndata'), '$.CapFrStd') CapFrStd,
            get_json_object(get_json_object(line,'turndata'), '$.QpMax') QpMax,
            get_json_object(get_json_object(line,'turndata'), '$.QpMin') QpMin,
            get_json_object(get_json_object(line,'turndata'), '$.QpMean') QpMean,
            get_json_object(get_json_object(line,'turndata'), '$.QpStd') QpStd,
            get_json_object(get_json_object(line,'turndata'), '$.SendFrMax') SendFrMax,
            get_json_object(get_json_object(line,'turndata'), '$.SendFrMin') SendFrMin,
            get_json_object(get_json_object(line,'turndata'), '$.SendFrMean') SendFrMean,
            get_json_object(get_json_object(line,'turndata'), '$.SendFrStd') SendFrStd,
            get_json_object(get_json_object(line,'turndata'), '$.TarRateMax') TarRateMax,
            get_json_object(get_json_object(line,'turndata'), '$.TarRateMin') TarRateMin,
            get_json_object(get_json_object(line,'turndata'), '$.TarRateMean') TarRateMean,
            get_json_object(get_json_object(line,'turndata'), '$.TarRateStd') TarRateStd,
            get_json_object(get_json_object(line,'turndata'), '$.ActRateMax') ActRateMax,
            get_json_object(get_json_object(line,'turndata'), '$.ActRateMin') ActRateMin,
            get_json_object(get_json_object(line,'turndata'), '$.ActRateMean') ActRateMean,
            get_json_object(get_json_object(line,'turndata'), '$.ActRateStd') ActRateStd,
            get_json_object(get_json_object(line,'turndata'), '$.RttMax') RttMax,
            get_json_object(get_json_object(line,'turndata'), '$.RttMin') RttMin,
            get_json_object(get_json_object(line,'turndata'), '$.RttMean') RttMean,
            get_json_object(get_json_object(line,'turndata'), '$.RttStd') RttStd,
            get_json_object(get_json_object(line,'turndata'), '$.LossMax') LossMax,
            get_json_object(get_json_object(line,'turndata'), '$.LossMin') LossMin,
            get_json_object(get_json_object(line,'turndata'), '$.LossMean') LossMean,
            get_json_object(get_json_object(line,'turndata'), '$.LossStd') LossStd,
            get_json_object(get_json_object(line,'turndata'), '$.RlLossMax') RlLossMax,
            get_json_object(get_json_object(line,'turndata'), '$.RlLossMin') RlLossMin,
            get_json_object(get_json_object(line,'turndata'), '$.RlLossMean') RlLossMean,
            get_json_object(get_json_object(line,'turndata'), '$.RlLossStd') RlLossStd,
            get_json_object(get_json_object(line,'turndata'), '$.SendRateMax') SendRateMax,
            get_json_object(get_json_object(line,'turndata'), '$.SendRateMin') SendRateMin,
            get_json_object(get_json_object(line,'turndata'), '$.SendRateMean') SendRateMean,
            get_json_object(get_json_object(line,'turndata'), '$.SendRateStd') SendRateStd
        from rtc_su
    )t1
    full outer join
    (
        select
            get_json_object(line, '$.connectionid') connectionid,
            get_json_object(get_json_object(line,'$.turndata'),'$.rollback') roll,
            get_json_object(get_json_object(line,'turndata'), '$.ConnTime') ConnTime,
            get_json_object(get_json_object(line,'turndata'), '$.ConnState') ConnState,
            get_json_object(get_json_object(line,'turndata'), '$.ConnType') ConnType,
            get_json_object(get_json_object(line,'turndata'), '$.GatherState') GatherState,
            get_json_object(get_json_object(line,'turndata'), '$.SigState') SigState,
            get_json_object(get_json_object(line,'turndata'), '$.IceState') IceState,
            get_json_object(get_json_object(line,'turndata'), '$.StreamId') StreamId,
            get_json_object(get_json_object(line,'turndata'), '$.IsRTC') IsRTC,
            get_json_object(get_json_object(line,'turndata'), '$.EBytes') EBytes,
            get_json_object(get_json_object(line,'turndata'), '$.HBytes') HBytes,
            get_json_object(get_json_object(line,'turndata'), '$.PBytes') PBytes,
            get_json_object(get_json_object(line,'turndata'), '$.RBytes') RBytes,
            get_json_object(get_json_object(line,'turndata'), '$.Enc') Enc,
            get_json_object(get_json_object(line,'turndata'), '$.CodecName') CodecName,
            get_json_object(get_json_object(line,'turndata'), '$.LocalIpport') LocalIpport,
            get_json_object(get_json_object(line,'turndata'), '$.LocalProto') LocalProto,
            get_json_object(get_json_object(line,'turndata'), '$.LocalType') LocalType,
            get_json_object(get_json_object(line,'turndata'), '$.RemotePort') RemotePort,
            get_json_object(get_json_object(line,'turndata'), '$.RemoteProto') RemoteProto,
            get_json_object(get_json_object(line,'turndata'), '$.RemoteType') RemoteType,
            get_json_object(get_json_object(line,'turndata'), '$.CpuMax') CpuMax,
            get_json_object(get_json_object(line,'turndata'), '$.CpuMin') CpuMin,
            get_json_object(get_json_object(line,'turndata'), '$.CpuMean') CpuMean,
            get_json_object(get_json_object(line,'turndata'), '$.CpuStd') CpuStd,
            get_json_object(get_json_object(line,'turndata'), '$.CapFrMax') CapFrMax,
            get_json_object(get_json_object(line,'turndata'), '$.CapFrMin') CapFrMin,
            get_json_object(get_json_object(line,'turndata'), '$.CapFrMean') CapFrMean,
            get_json_object(get_json_object(line,'turndata'), '$.CapFrStd') CapFrStd,
            get_json_object(get_json_object(line,'turndata'), '$.QpMax') QpMax,
            get_json_object(get_json_object(line,'turndata'), '$.QpMin') QpMin,
            get_json_object(get_json_object(line,'turndata'), '$.QpMean') QpMean,
            get_json_object(get_json_object(line,'turndata'), '$.QpStd') QpStd,
            get_json_object(get_json_object(line,'turndata'), '$.SendFrMax') SendFrMax,
            get_json_object(get_json_object(line,'turndata'), '$.SendFrMin') SendFrMin,
            get_json_object(get_json_object(line,'turndata'), '$.SendFrMean') SendFrMean,
            get_json_object(get_json_object(line,'turndata'), '$.SendFrStd') SendFrStd,
            get_json_object(get_json_object(line,'turndata'), '$.TarRateMax') TarRateMax,
            get_json_object(get_json_object(line,'turndata'), '$.TarRateMin') TarRateMin,
            get_json_object(get_json_object(line,'turndata'), '$.TarRateMean') TarRateMean,
            get_json_object(get_json_object(line,'turndata'), '$.TarRateStd') TarRateStd,
            get_json_object(get_json_object(line,'turndata'), '$.ActRateMax') ActRateMax,
            get_json_object(get_json_object(line,'turndata'), '$.ActRateMin') ActRateMin,
            get_json_object(get_json_object(line,'turndata'), '$.ActRateMean') ActRateMean,
            get_json_object(get_json_object(line,'turndata'), '$.ActRateStd') ActRateStd,
            get_json_object(get_json_object(line,'turndata'), '$.RttMax') RttMax,
            get_json_object(get_json_object(line,'turndata'), '$.RttMin') RttMin,
            get_json_object(get_json_object(line,'turndata'), '$.RttMean') RttMean,
            get_json_object(get_json_object(line,'turndata'), '$.RttStd') RttStd,
            get_json_object(get_json_object(line,'turndata'), '$.LossMax') LossMax,
            get_json_object(get_json_object(line,'turndata'), '$.LossMin') LossMin,
            get_json_object(get_json_object(line,'turndata'), '$.LossMean') LossMean,
            get_json_object(get_json_object(line,'turndata'), '$.LossStd') LossStd,
            get_json_object(get_json_object(line,'turndata'), '$.RlLossMax') RlLossMax,
            get_json_object(get_json_object(line,'turndata'), '$.RlLossMin') RlLossMin,
            get_json_object(get_json_object(line,'turndata'), '$.RlLossMean') RlLossMean,
            get_json_object(get_json_object(line,'turndata'), '$.RlLossStd') RlLossStd,
            get_json_object(get_json_object(line,'turndata'), '$.SendRateMax') SendRateMax,
            get_json_object(get_json_object(line,'turndata'), '$.SendRateMin') SendRateMin,
            get_json_object(get_json_object(line,'turndata'), '$.SendRateMean') SendRateMean,
            get_json_object(get_json_object(line,'turndata'), '$.SendRateStd') SendRateStd
        from  rtc_du
    )t2
    on t1.connectionid=t2.connectionid
)
insert overwrite table ${APP}.dwd_sdk_du partition (dt = '$do_date')
select \`(r1)?+.+\`,'',
       max(r2) over (partition by connectionid)
from (
         select *,
                row_number() over (partition by connectionid order by r1) r2
         from (
                  select connectionid,
                         city,
                         platform,
                         version,
                         clientype,
                         network,
                         accountid,
                         resolution,
                         vendor,
                         clienttime,
                         get_json_object(turndata_json, '$.ConnTime'),
                         get_json_object(turndata_json, '$.ConnState'),
                         get_json_object(turndata_json, '$.ConnType'),
                         get_json_object(turndata_json, '$.GatherState'),
                         get_json_object(turndata_json, '$.SigState'),
                         get_json_object(turndata_json, '$.IceState'),
                         get_json_object(turndata_json, '$.StreamId'),
                         get_json_object(turndata_json, '$.IsRTC'),
                         get_json_object(turndata_json, '$.EBytes'),
                         get_json_object(turndata_json, '$.HBytes'),
                         get_json_object(turndata_json, '$.PBytes'),
                         get_json_object(turndata_json, '$.RBytes'),
                         get_json_object(turndata_json, '$.Enc'),
                         get_json_object(turndata_json, '$.CodecName'),
                         get_json_object(turndata_json, '$.LocalIpport'),
                         get_json_object(turndata_json, '$.LocalProto'),
                         get_json_object(turndata_json, '$.LocalType'),
                         get_json_object(turndata_json, '$.RemotePort'),
                         get_json_object(turndata_json, '$.RemoteProto'),
                         get_json_object(turndata_json, '$.RemoteType'),
                         get_json_object(turndata_json, '$.CpuMax'),
                         get_json_object(turndata_json, '$.CpuMin'),
                         get_json_object(turndata_json, '$.CpuMean'),
                         get_json_object(turndata_json, '$.CpuStd'),
                         get_json_object(turndata_json, '$.CapFrMax'),
                         get_json_object(turndata_json, '$.CapFrMin'),
                         get_json_object(turndata_json, '$.CapFrMean'),
                         get_json_object(turndata_json, '$.CapFrStd'),
                         get_json_object(turndata_json, '$.QpMax'),
                         get_json_object(turndata_json, '$.QpMin'),
                         get_json_object(turndata_json, '$.QpMean'),
                         get_json_object(turndata_json, '$.QpStd'),
                         get_json_object(turndata_json, '$.SendFrMax'),
                         get_json_object(turndata_json, '$.SendFrMin'),
                         get_json_object(turndata_json, '$.SendFrMean'),
                         get_json_object(turndata_json, '$.SendFrStd'),
                         get_json_object(turndata_json, '$.TarRateMax'),
                         get_json_object(turndata_json, '$.TarRateMin'),
                         get_json_object(turndata_json, '$.TarRateMean'),
                         get_json_object(turndata_json, '$.TarRateStd'),
                         get_json_object(turndata_json, '$.ActRateMax'),
                         get_json_object(turndata_json, '$.ActRateMin'),
                         get_json_object(turndata_json, '$.ActRateMean'),
                         get_json_object(turndata_json, '$.ActRateStd'),
                         get_json_object(turndata_json, '$.RttMax'),
                         get_json_object(turndata_json, '$.RttMin'),
                         get_json_object(turndata_json, '$.RttMean'),
                         get_json_object(turndata_json, '$.RttStd'),
                         get_json_object(turndata_json, '$.LossMax'),
                         get_json_object(turndata_json, '$.LossMin'),
                         get_json_object(turndata_json, '$.LossMean'),
                         get_json_object(turndata_json, '$.LossStd'),
                         get_json_object(turndata_json, '$.RlLossMax'),
                         get_json_object(turndata_json, '$.RlLossMin'),
                         get_json_object(turndata_json, '$.RlLossMean'),
                         get_json_object(turndata_json, '$.RlLossStd'),
                         get_json_object(turndata_json, '$.SendRateMax'),
                         get_json_object(turndata_json, '$.SendRateMin'),
                         get_json_object(turndata_json, '$.SendRateMean'),
                         get_json_object(turndata_json, '$.SendRateStd'),
                         logtime,
                         row_number() over (partition by connectionid) r1
                  from (
                           select get_json_object(line, '$.connectionid') connectionid,
                                  get_json_object(line, '$.city')         city,
                                  get_json_object(line, '$.platform')     platform,
                                  get_json_object(line, '$.version')      version,
                                  get_json_object(line, '$.clientype')    clientype,
                                  get_json_object(line, '$.network')      network,
                                  get_json_object(line, '$.accountid')    accountid,
                                  get_json_object(line, '$.resolution')   resolution,
                                  get_json_object(line, '$.vendor')       vendor,
                                  get_json_object(line, '$.clienttime')   clienttime,
                                  get_json_object(line, '$.logtime')      logtime,
                                  turndata_json
                           from (
                                    select line
                                    from ${APP}.ods_tuevent_log
                                    where dt = '$do_date'
                                      and get_json_object(line, '$.logtype') = 'tuevent'
                                      and get_json_object(line, '$.events') = 'link_info'
                                      and get_json_object(line, '$.clientype') = 'host'
                                      and get_json_object(line, '$.connectionid') <> ''
                                      and get_json_object(line, '$.turndata') like '[%'
                                      and get_json_object(line, '$.version') < '4.5.0.0'
                                      and size(split(regexp_replace(
                                                             regexp_replace(get_json_object(line, '$.turndata'), '\\\[|\\\]', ''),
                                                             '\\\}\\\,\\\{', '\\\}\\\;\\\{'), '\\\;')) >
                                          0 
                                    group by line
                                ) t6
                                    lateral view explode(split(
                                            regexp_replace(
                                                    regexp_replace(get_json_object(line, '$.turndata'), '\\\[|\\\]', ''),
                                                    '\\\}\\\,\\\{', '\\\}\\\;\\\{'),
                                            '\\\;')) ods_tuevent_log_tmp as turndata_json
                       ) t7
                  union all
                  select t5.connectionid,
                         t5.city,
                         t5.platform,
                         t5.version,
                         t5.clientype,
                         t5.network,
                         t5.accountid,
                         t5.resolution,
                         t5.vendor,
                         t5.clienttime,
                         get_json_object(t5.turndata_json, '$.ConnTime'),
                         get_json_object(t5.turndata_json, '$.ConnState'),
                         get_json_object(t5.turndata_json, '$.ConnType'),
                         get_json_object(t5.turndata_json, '$.GatherState'),
                         get_json_object(t5.turndata_json, '$.SigState'),
                         get_json_object(t5.turndata_json, '$.IceState'),
                         get_json_object(t5.turndata_json, '$.StreamId'),
                         get_json_object(t5.turndata_json, '$.IsRTC'),
                         get_json_object(t5.turndata_json, '$.EBytes'),
                         get_json_object(t5.turndata_json, '$.HBytes'),
                         get_json_object(t5.turndata_json, '$.PBytes'),
                         get_json_object(t5.turndata_json, '$.RBytes'),
                         get_json_object(t5.turndata_json, '$.Enc'),
                         get_json_object(t5.turndata_json, '$.CodecName'),
                         get_json_object(t5.turndata_json, '$.LocalIpport'),
                         get_json_object(t5.turndata_json, '$.LocalProto'),
                         get_json_object(t5.turndata_json, '$.LocalType'),
                         get_json_object(t5.turndata_json, '$.RemotePort'),
                         get_json_object(t5.turndata_json, '$.RemoteProto'),
                         get_json_object(t5.turndata_json, '$.RemoteType'),
                         get_json_object(t5.turndata_json, '$.CpuMax'),
                         get_json_object(t5.turndata_json, '$.CpuMin'),
                         get_json_object(t5.turndata_json, '$.CpuMean'),
                         get_json_object(t5.turndata_json, '$.CpuStd'),
                         get_json_object(t5.turndata_json, '$.CapFrMax'),
                         get_json_object(t5.turndata_json, '$.CapFrMin'),
                         get_json_object(t5.turndata_json, '$.CapFrMean'),
                         get_json_object(t5.turndata_json, '$.CapFrStd'),
                         get_json_object(t5.turndata_json, '$.QpMax'),
                         get_json_object(t5.turndata_json, '$.QpMin'),
                         get_json_object(t5.turndata_json, '$.QpMean'),
                         get_json_object(t5.turndata_json, '$.QpStd'),
                         get_json_object(t5.turndata_json, '$.SendFrMax'),
                         get_json_object(t5.turndata_json, '$.SendFrMin'),
                         get_json_object(t5.turndata_json, '$.SendFrMean'),
                         get_json_object(t5.turndata_json, '$.SendFrStd'),
                         get_json_object(t5.turndata_json, '$.TarRateMax'),
                         get_json_object(t5.turndata_json, '$.TarRateMin'),
                         get_json_object(t5.turndata_json, '$.TarRateMean'),
                         get_json_object(t5.turndata_json, '$.TarRateStd'),
                         get_json_object(t5.turndata_json, '$.ActRateMax'),
                         get_json_object(t5.turndata_json, '$.ActRateMin'),
                         get_json_object(t5.turndata_json, '$.ActRateMean'),
                         get_json_object(t5.turndata_json, '$.ActRateStd'),
                         get_json_object(t5.turndata_json, '$.RttMax'),
                         get_json_object(t5.turndata_json, '$.RttMin'),
                         get_json_object(t5.turndata_json, '$.RttMean'),
                         get_json_object(t5.turndata_json, '$.RttStd'),
                         get_json_object(t5.turndata_json, '$.LossMax'),
                         get_json_object(t5.turndata_json, '$.LossMin'),
                         get_json_object(t5.turndata_json, '$.LossMean'),
                         get_json_object(t5.turndata_json, '$.LossStd'),
                         get_json_object(t5.turndata_json, '$.RlLossMax'),
                         get_json_object(t5.turndata_json, '$.RlLossMin'),
                         get_json_object(t5.turndata_json, '$.RlLossMean'),
                         get_json_object(t5.turndata_json, '$.RlLossStd'),
                         get_json_object(t5.turndata_json, '$.SendRateMax'),
                         get_json_object(t5.turndata_json, '$.SendRateMin'),
                         get_json_object(t5.turndata_json, '$.SendRateMean'),
                         get_json_object(t5.turndata_json, '$.SendRateStd'),
                         t5.logtime,
                         9999
                  from (
                           select get_json_object(line, '$.connectionid') conn_id
                           from (
                                    select line
                                    from ${APP}.ods_tuevent_log
                                    where dt = '$do_date'
                                      and get_json_object(line, '$.logtype') = 'tuevent'
                                      and get_json_object(line, '$.events') = 'link_info'
                                      and get_json_object(line, '$.clientype') = 'host'
                                      and get_json_object(line, '$.connectionid') <> ''
                                      and get_json_object(line, '$.turndata') like '[%'
                                      and get_json_object(line, '$.version') < '4.5.0.0'
                                      and size(split(regexp_replace(
                                                             regexp_replace(get_json_object(line, '$.turndata'), '\\\[|\\\]', ''),
                                                             '\\\}\\\,\\\{', '\\\}\\\;\\\{'), '\\\;')) >
                                          0 
                                    group by line
                                ) t1
                       ) t3
                           left join 
                      (
                          select get_json_object(line, '$.connectionid') connectionid,
                                 get_json_object(line, '$.city')         city,
                                 get_json_object(line, '$.platform')     platform,
                                 get_json_object(line, '$.version')      version,
                                 get_json_object(line, '$.clientype')    clientype,
                                 get_json_object(line, '$.network')      network,
                                 get_json_object(line, '$.accountid')    accountid,
                                 get_json_object(line, '$.resolution')   resolution,
                                 get_json_object(line, '$.vendor')       vendor,
                                 get_json_object(line, '$.clienttime')   clienttime,
                                 get_json_object(line, '$.logtime')      logtime,
                                 get_json_object(line, '$.turndata')     turndata_json
                          from (
                                   select line
                                   from ${APP}.ods_tuevent_log
                                   where dt = '$do_date'
                                     and get_json_object(line, '$.logtype') = 'tuevent'
                                     and get_json_object(line, '$.events') = 'link_info'
                                     and get_json_object(line, '$.clientype') = 'client'
                                     and get_json_object(line, '$.connectionid') <> ''
                                     and get_json_object(line, '$.version') < '4.5.0.0'
                                     and get_json_object(get_json_object(line, '$.turndata'), '$.IsRTC') = '0'
                                   group by line
                               ) t4
                      ) t5
                                     on t3.conn_id = t5.connectionid
                  where t5.connectionid is not null
              ) t8
     ) t9
union all
select
    *,max(r1) over(partition by connectionid)
from
(
    select
        connectionid,city,platform,version,clientype,network,accountid,resolution,vendor,clienttime,
        ConnTime,ConnState,ConnType,GatherState,SigState,IceState,StreamId,IsRTC,EBytes,HBytes,PBytes,RBytes,
        get_json_object(tracks_json,'$.Enc'),
        get_json_object(tracks_json,'$.CodecName'),
        LocalIpport,LocalProto,LocalType,RemotePort,RemoteProto,RemoteType,CpuMax,CpuMin,CpuMean,CpuStd,
        get_json_object(tracks_json,'$.CapFrMax'),
        get_json_object(tracks_json,'$.CapFrMin'),
        get_json_object(tracks_json,'$.CapFrMean'),
        get_json_object(tracks_json,'$.CapFrStd'),
        get_json_object(tracks_json,'$.QpMax'),
        get_json_object(tracks_json,'$.QpMin'),
        get_json_object(tracks_json,'$.QpMean'),
        get_json_object(tracks_json,'$.QpStd'),
        get_json_object(tracks_json,'$.SendFrMax'),
        get_json_object(tracks_json,'$.SendFrMin'),
        get_json_object(tracks_json,'$.SendFrMean'),
        get_json_object(tracks_json,'$.SendFrStd'),
        TarRateMax,TarRateMin,TarRateMean,TarRateStd,ActRateMax,ActRateMin,ActRateMean,ActRateStd,RttMax,RttMin,RttMean,RttStd,
        LossMax,LossMin,LossMean,LossStd,RlLossMax,RlLossMin,RlLossMean,RlLossStd,SendRateMax,SendRateMin,SendRateMean,SendRateStd,
        logtime,r1,roll
    from
    (
        select
            connectionid,city,platform,version,clientype,network,accountid,resolution,vendor,clienttime,roll,logtime,
            row_number() over (partition by connectionid) r1,get_json_object(turndata_json,'$.tracks') tr,
            get_json_object(turndata_json, '$.ConnTime') ConnTime,
            get_json_object(turndata_json, '$.ConnState') ConnState,
            get_json_object(turndata_json, '$.ConnType') ConnType,
            get_json_object(turndata_json, '$.GatherState') GatherState,
            get_json_object(turndata_json, '$.SigState') SigState,
            get_json_object(turndata_json, '$.IceState') IceState,
            get_json_object(turndata_json, '$.StreamId') StreamId,
            get_json_object(turndata_json, '$.IsRTC') IsRTC,
            get_json_object(turndata_json, '$.EBytes') EBytes,
            get_json_object(turndata_json, '$.HBytes') HBytes,
            get_json_object(turndata_json, '$.PBytes') PBytes,
            get_json_object(turndata_json, '$.RBytes') RBytes,
            get_json_object(turndata_json, '$.LocalIpport') LocalIpport,
            get_json_object(turndata_json, '$.LocalProto') LocalProto,
            get_json_object(turndata_json, '$.LocalType') LocalType,
            get_json_object(turndata_json, '$.RemotePort') RemotePort,
            get_json_object(turndata_json, '$.RemoteProto') RemoteProto,
            get_json_object(turndata_json, '$.RemoteType') RemoteType,
            get_json_object(turndata_json, '$.CpuMax') CpuMax,
            get_json_object(turndata_json, '$.CpuMin') CpuMin,
            get_json_object(turndata_json, '$.CpuMean') CpuMean,
            get_json_object(turndata_json, '$.CpuStd') CpuStd,
            get_json_object(turndata_json, '$.TarRateMax') TarRateMax,
            get_json_object(turndata_json, '$.TarRateMin') TarRateMin,
            get_json_object(turndata_json, '$.TarRateMean') TarRateMean,
            get_json_object(turndata_json, '$.TarRateStd') TarRateStd,
            get_json_object(turndata_json, '$.ActRateMax') ActRateMax,
            get_json_object(turndata_json, '$.ActRateMin') ActRateMin,
            get_json_object(turndata_json, '$.ActRateMean') ActRateMean,
            get_json_object(turndata_json, '$.ActRateStd') ActRateStd,
            get_json_object(turndata_json, '$.RttMax') RttMax,
            get_json_object(turndata_json, '$.RttMin') RttMin,
            get_json_object(turndata_json, '$.RttMean') RttMean,
            get_json_object(turndata_json, '$.RttStd') RttStd,
            get_json_object(turndata_json, '$.LossMax') LossMax,
            get_json_object(turndata_json, '$.LossMin') LossMin,
            get_json_object(turndata_json, '$.LossMean') LossMean,
            get_json_object(turndata_json, '$.LossStd') LossStd,
            get_json_object(turndata_json, '$.RlLossMax') RlLossMax,
            get_json_object(turndata_json, '$.RlLossMin') RlLossMin,
            get_json_object(turndata_json, '$.RlLossMean') RlLossMean,
            get_json_object(turndata_json, '$.RlLossStd') RlLossStd,
            get_json_object(turndata_json, '$.SendRateMax') SendRateMax,
            get_json_object(turndata_json, '$.SendRateMin') SendRateMin,
            get_json_object(turndata_json, '$.SendRateMean') SendRateMean,
            get_json_object(turndata_json, '$.SendRateStd') SendRateStd
        from
        (
            select
                t0.*
            from
            (
                select
                    get_json_object(line, '$.connectionid') connectionid,
                    get_json_object(line, '$.city')         city,
                    get_json_object(line, '$.platform')     platform,
                    get_json_object(line, '$.version')      version,
                    get_json_object(line, '$.clientype')    clientype,
                    get_json_object(line, '$.network')      network,
                    get_json_object(line, '$.accountid')    accountid,
                    get_json_object(line, '$.resolution')   resolution,
                    get_json_object(line, '$.vendor')       vendor,
                    get_json_object(line, '$.clienttime')   clienttime,
                    get_json_object(get_json_object(line,'$.turndata'),'$.rollback') roll,
                    get_json_object(line, '$.logtime')      logtime,
                    get_json_object(get_json_object(line,'$.turndata'),'$.licode')  licode_json
                from rtc_du
                where (get_json_object(line, '$.version') >= '4.5.0.0' or get_json_object(line, '$.platform') in('window_master','window_slave') )
                and (get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') <> 'AuthFail' or get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') is null)
                and get_json_object(get_json_object(line, '$.turndata'),'$.rollback') in ('0','1')
                and get_json_object(get_json_object(line, '$.turndata'),'$.licode') is not null
            )t0
            left join rtc_etl
            on t0.connectionid = rtc_etl.connid
            where (rtc_etl.su_roll=rtc_etl.du_roll or (du_roll is not null and su_roll is null ))
        )t1
            lateral view explode(split(
                                    regexp_replace(
                                            regexp_replace(licode_json, '\\\[\\\[|\\\]\\\]', ''),
                                            '\\\}\\\,\\\{\\\"ActRateMax', '\\\}\\\!!!\\\{\\\"ActRateMax'),
                                    '\\\!!!')) ods_tuevent_log_tmp as turndata_json
    )t2
        lateral view explode(split(
                                    regexp_replace(
                                            regexp_replace(tr, '\\\[|\\\]', ''),
                                            '\\\}\\\,\\\{\\\"CapFrMax', '\\\}\\\!!!\\\{\\\"CapFrMax'),
                                    '\\\!!!')) ods_tuevent_log_tmp as tracks_json
    union all
    select
        connectionid,
        city,
        platform,
        version,
        clientype,
        network,
        accountid,
        resolution,
        vendor,
        clienttime,
        rtc_etl.ConnTime,
        rtc_etl.ConnState,
        rtc_etl.ConnType,
        rtc_etl.GatherState,
        rtc_etl.SigState,
        rtc_etl.IceState,
        rtc_etl.StreamId,
        rtc_etl.IsRTC,
        rtc_etl.EBytes,
        rtc_etl.HBytes,
        rtc_etl.PBytes,
        rtc_etl.RBytes,
        rtc_etl.Enc,
        rtc_etl.CodecName,
        rtc_etl.LocalIpport,
        rtc_etl.LocalProto,
        rtc_etl.LocalType,
        rtc_etl.RemotePort,
        rtc_etl.RemoteProto,
        rtc_etl.RemoteType,
        rtc_etl.CpuMax,
        rtc_etl.CpuMin,
        rtc_etl.CpuMean,
        rtc_etl.CpuStd,
        rtc_etl.CapFrMax,
        rtc_etl.CapFrMin,
        rtc_etl.CapFrMean,
        rtc_etl.CapFrStd,
        rtc_etl.QpMax,
        rtc_etl.QpMin,
        rtc_etl.QpMean,
        rtc_etl.QpStd,
        rtc_etl.SendFrMax,
        rtc_etl.SendFrMin,
        rtc_etl.SendFrMean,
        rtc_etl.SendFrStd,
        rtc_etl.TarRateMax,
        rtc_etl.TarRateMin,
        rtc_etl.TarRateMean,
        rtc_etl.TarRateStd,
        rtc_etl.ActRateMax,
        rtc_etl.ActRateMin,
        rtc_etl.ActRateMean,
        rtc_etl.ActRateStd,
        rtc_etl.RttMax,
        rtc_etl.RttMin,
        rtc_etl.RttMean,
        rtc_etl.RttStd,
        rtc_etl.LossMax,
        rtc_etl.LossMin,
        rtc_etl.LossMean,
        rtc_etl.LossStd,
        rtc_etl.RlLossMax,
        rtc_etl.RlLossMin,
        rtc_etl.RlLossMean,
        rtc_etl.RlLossStd,
        rtc_etl.SendRateMax,
        rtc_etl.SendRateMin,
        rtc_etl.SendRateMean,
        rtc_etl.SendRateStd,
        logtime,
        9999,
        roll
    from
    (
        select
            get_json_object(line, '$.connectionid') connectionid,
            get_json_object(line, '$.city')         city,
            get_json_object(line, '$.platform')     platform,
            get_json_object(line, '$.version')      version,
            get_json_object(line, '$.clientype')    clientype,
            get_json_object(line, '$.network')      network,
            get_json_object(line, '$.accountid')    accountid,
            get_json_object(line, '$.resolution')   resolution,
            get_json_object(line, '$.vendor')       vendor,
            get_json_object(line, '$.clienttime')   clienttime,
            get_json_object(line, '$.logtime') logtime,
            get_json_object(line, '$.rollback') roll
        from rtc_du
        where (get_json_object(line, '$.version') >= '4.5.0.0' or get_json_object(line, '$.platform') in('window_master','window_slave') )
        and (get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') <> 'AuthFail' or get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') is null)
        and get_json_object(get_json_object(line, '$.turndata'),'$.rollback') ='1'
    )t0
    left join rtc_etl
    on t0.connectionid = rtc_etl.connid
    where (rtc_etl.su_roll=rtc_etl.du_roll or (du_roll is not null and su_roll is null )) 
)t1;"


dwd_sdk_su="
set hive.support.quoted.identifiers=None;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
with
rtc_su as
(
    select
        line
    from ${APP}.ods_tuevent_log
    where dt = '$do_date'
    and get_json_object(line, '$.events') = 'link_info'
    and get_json_object(line, '$.clientype') = 'client'
    and get_json_object(line, '$.connectionid') <> ''
),
rtc_du as
(
    select
        line
    from ${APP}.ods_tuevent_log
    where dt = '$do_date'
    and get_json_object(line, '$.events') = 'link_info'
    and get_json_object(line, '$.clientype') = 'host'
    and get_json_object(line, '$.connectionid') <> ''
),
rtc_etl as
(
    select 
        nvl(t1.connectionid,t2.connectionid) connid,t1.roll su_roll,t2.roll du_roll,
        nvl(t1.ConnTime,t2.ConnTime) ConnTime,
        nvl(t1.ConnState,t2.ConnState) ConnState,
        nvl(t1.ConnType,t2.ConnType) ConnType,
        nvl(t1.GatherState,t2.GatherState) GatherState,
        nvl(t1.SigState,t2.SigState) SigState,
        nvl(t1.IceState,t2.IceState) IceState,
        nvl(t1.StreamId,t2.StreamId) StreamId,
        nvl(t1.IsRTC,t2.IsRTC) IsRTC,
        nvl(t1.EBytes,t2.EBytes) EBytes,
        nvl(t1.HBytes,t2.HBytes) HBytes,
        nvl(t1.PBytes,t2.PBytes) PBytes,
        nvl(t1.RBytes,t2.RBytes) RBytes,
        nvl(t1.Fst1,t2.Fst1) Fst1,
        nvl(t1.Fst2,t2.Fst2) Fst2,
        nvl(t1.Fst3,t2.Fst3) Fst3,
        nvl(t1.Fst4,t2.Fst4) Fst4,
        nvl(t1.LocalIpport,t2.LocalIpport) LocalIpport,
        nvl(t1.LocalProto,t2.LocalProto) LocalProto,
        nvl(t1.LocalType,t2.LocalType) LocalType,
        nvl(t1.RemotePort,t2.RemotePort) RemotePort,
        nvl(t1.RemoteProto,t2.RemoteProto) RemoteProto,
        nvl(t1.RemoteType,t2.RemoteType) RemoteType,
        nvl(t1.RcvdFrMax,t2.RcvdFrMax) RcvdFrMax,
        nvl(t1.RcvdFrMin,t2.RcvdFrMin) RcvdFrMin,
        nvl(t1.RcvdFrMean,t2.RcvdFrMean) RcvdFrMean,
        nvl(t1.RcvdFrStd,t2.RcvdFrStd) RcvdFrStd,
        nvl(t1.DecFrMax,t2.DecFrMax) DecFrMax,
        nvl(t1.DecFrMin,t2.DecFrMin) DecFrMin,
        nvl(t1.DecFrMean,t2.DecFrMean) DecFrMean,
        nvl(t1.DecFrStd,t2.DecFrStd) DecFrStd,
        nvl(t1.RendFrMax,t2.RendFrMax) RendFrMax,
        nvl(t1.RendFrMin,t2.RendFrMin) RendFrMin,
        nvl(t1.RendFrMean,t2.RendFrMean) RendFrMean,
        nvl(t1.RendFrStd,t2.RendFrStd) RendFrStd,
        nvl(t1.RcvdRateMax,t2.RcvdRateMax) RcvdRateMax,
        nvl(t1.RcvdRateMin,t2.RcvdRateMin) RcvdRateMin,
        nvl(t1.RcvdRateMean,t2.RcvdRateMean) RcvdRateMean,
        nvl(t1.RcvdRateStd,t2.RcvdRateStd) RcvdRateStd,
        nvl(t1.DecStuckTime,t2.DecStuckTime) DecStuckTime,
        nvl(t1.DecStuckCnt,t2.DecStuckCnt) DecStuckCnt,
        nvl(t1.LDecStuckTime,t2.LDecStuckTime) LDecStuckTime,
        nvl(t1.LDecStuckCnt,t2.LDecStuckCnt) LDecStuckCnt,
        nvl(t1.FirstFrameTime,t2.FirstFrameTime) FirstFrameTime,
        nvl(t1.LastFrameTime,t2.LastFrameTime) LastFrameTime
    from
    (
        select
            get_json_object(line, '$.connectionid') connectionid,
            get_json_object(get_json_object(line,'$.turndata'),'$.rollback') roll,
            get_json_object(get_json_object(line,'$.turndata'), '$.ConnTime') ConnTime,
            get_json_object(get_json_object(line,'$.turndata'), '$.ConnState') ConnState,
            get_json_object(get_json_object(line,'$.turndata'), '$.ConnType') ConnType,
            get_json_object(get_json_object(line,'$.turndata'), '$.GatherState') GatherState,
            get_json_object(get_json_object(line,'$.turndata'), '$.SigState') SigState,
            get_json_object(get_json_object(line,'$.turndata'), '$.IceState') IceState,
            get_json_object(get_json_object(line,'$.turndata'), '$.StreamId') StreamId,
            get_json_object(get_json_object(line,'$.turndata'), '$.IsRTC') IsRTC,
            get_json_object(get_json_object(line,'$.turndata'), '$.EBytes') EBytes,
            get_json_object(get_json_object(line,'$.turndata'), '$.HBytes') HBytes,
            get_json_object(get_json_object(line,'$.turndata'), '$.PBytes') PBytes,
            get_json_object(get_json_object(line,'$.turndata'), '$.RBytes') RBytes,
            get_json_object(get_json_object(line,'$.turndata'), '$.Fst1') Fst1,
            get_json_object(get_json_object(line,'$.turndata'), '$.Fst2') Fst2,
            get_json_object(get_json_object(line,'$.turndata'), '$.Fst3') Fst3,
            get_json_object(get_json_object(line,'$.turndata'), '$.Fst4') Fst4,
            get_json_object(get_json_object(line,'$.turndata'), '$.LocalIpport') LocalIpport,
            get_json_object(get_json_object(line,'$.turndata'), '$.LocalProto') LocalProto,
            get_json_object(get_json_object(line,'$.turndata'), '$.LocalType') LocalType,
            get_json_object(get_json_object(line,'$.turndata'), '$.RemotePort') RemotePort,
            get_json_object(get_json_object(line,'$.turndata'), '$.RemoteProto') RemoteProto,
            get_json_object(get_json_object(line,'$.turndata'), '$.RemoteType') RemoteType,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdFrMax') RcvdFrMax,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdFrMin') RcvdFrMin,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdFrMean') RcvdFrMean,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdFrStd') RcvdFrStd,
            get_json_object(get_json_object(line,'$.turndata'), '$.DecFrMax') DecFrMax,
            get_json_object(get_json_object(line,'$.turndata'), '$.DecFrMin') DecFrMin,
            get_json_object(get_json_object(line,'$.turndata'), '$.DecFrMean') DecFrMean,
            get_json_object(get_json_object(line,'$.turndata'), '$.DecFrStd') DecFrStd,
            get_json_object(get_json_object(line,'$.turndata'), '$.RendFrMax') RendFrMax,
            get_json_object(get_json_object(line,'$.turndata'), '$.RendFrMin') RendFrMin,
            get_json_object(get_json_object(line,'$.turndata'), '$.RendFrMean') RendFrMean,
            get_json_object(get_json_object(line,'$.turndata'), '$.RendFrStd') RendFrStd,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdRateMax') RcvdRateMax,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdRateMin') RcvdRateMin,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdRateMean') RcvdRateMean,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdRateStd') RcvdRateStd,
            get_json_object(get_json_object(line,'$.turndata'), '$.DecStuckTime') DecStuckTime,
            get_json_object(get_json_object(line,'$.turndata'), '$.DecStuckCnt') DecStuckCnt,
            get_json_object(get_json_object(line,'$.turndata'), '$.LDecStuckTime') LDecStuckTime,
            get_json_object(get_json_object(line,'$.turndata'), '$.LDecStuckCnt') LDecStuckCnt,
            get_json_object(get_json_object(line,'$.turndata'), '$.FirstFrameTime') FirstFrameTime,
            get_json_object(get_json_object(line,'$.turndata'), '$.LastFrameTime') LastFrameTime
        from rtc_su
    )t1
    full outer join
    (
        select
            get_json_object(line, '$.connectionid') connectionid,
            get_json_object(get_json_object(line,'$.turndata'),'$.rollback') roll,
            get_json_object(get_json_object(line,'$.turndata'), '$.ConnTime') ConnTime,
            get_json_object(get_json_object(line,'$.turndata'), '$.ConnState') ConnState,
            get_json_object(get_json_object(line,'$.turndata'), '$.ConnType') ConnType,
            get_json_object(get_json_object(line,'$.turndata'), '$.GatherState') GatherState,
            get_json_object(get_json_object(line,'$.turndata'), '$.SigState') SigState,
            get_json_object(get_json_object(line,'$.turndata'), '$.IceState') IceState,
            get_json_object(get_json_object(line,'$.turndata'), '$.StreamId') StreamId,
            get_json_object(get_json_object(line,'$.turndata'), '$.IsRTC') IsRTC,
            get_json_object(get_json_object(line,'$.turndata'), '$.EBytes') EBytes,
            get_json_object(get_json_object(line,'$.turndata'), '$.HBytes') HBytes,
            get_json_object(get_json_object(line,'$.turndata'), '$.PBytes') PBytes,
            get_json_object(get_json_object(line,'$.turndata'), '$.RBytes') RBytes,
            get_json_object(get_json_object(line,'$.turndata'), '$.Fst1') Fst1,
            get_json_object(get_json_object(line,'$.turndata'), '$.Fst2') Fst2,
            get_json_object(get_json_object(line,'$.turndata'), '$.Fst3') Fst3,
            get_json_object(get_json_object(line,'$.turndata'), '$.Fst4') Fst4,
            get_json_object(get_json_object(line,'$.turndata'), '$.LocalIpport') LocalIpport,
            get_json_object(get_json_object(line,'$.turndata'), '$.LocalProto') LocalProto,
            get_json_object(get_json_object(line,'$.turndata'), '$.LocalType') LocalType,
            get_json_object(get_json_object(line,'$.turndata'), '$.RemotePort') RemotePort,
            get_json_object(get_json_object(line,'$.turndata'), '$.RemoteProto') RemoteProto,
            get_json_object(get_json_object(line,'$.turndata'), '$.RemoteType') RemoteType,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdFrMax') RcvdFrMax,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdFrMin') RcvdFrMin,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdFrMean') RcvdFrMean,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdFrStd') RcvdFrStd,
            get_json_object(get_json_object(line,'$.turndata'), '$.DecFrMax') DecFrMax,
            get_json_object(get_json_object(line,'$.turndata'), '$.DecFrMin') DecFrMin,
            get_json_object(get_json_object(line,'$.turndata'), '$.DecFrMean') DecFrMean,
            get_json_object(get_json_object(line,'$.turndata'), '$.DecFrStd') DecFrStd,
            get_json_object(get_json_object(line,'$.turndata'), '$.RendFrMax') RendFrMax,
            get_json_object(get_json_object(line,'$.turndata'), '$.RendFrMin') RendFrMin,
            get_json_object(get_json_object(line,'$.turndata'), '$.RendFrMean') RendFrMean,
            get_json_object(get_json_object(line,'$.turndata'), '$.RendFrStd') RendFrStd,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdRateMax') RcvdRateMax,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdRateMin') RcvdRateMin,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdRateMean') RcvdRateMean,
            get_json_object(get_json_object(line,'$.turndata'), '$.RcvdRateStd') RcvdRateStd,
            get_json_object(get_json_object(line,'$.turndata'), '$.DecStuckTime') DecStuckTime,
            get_json_object(get_json_object(line,'$.turndata'), '$.DecStuckCnt') DecStuckCnt,
            get_json_object(get_json_object(line,'$.turndata'), '$.LDecStuckTime') LDecStuckTime,
            get_json_object(get_json_object(line,'$.turndata'), '$.LDecStuckCnt') LDecStuckCnt,
            get_json_object(get_json_object(line,'$.turndata'), '$.FirstFrameTime') FirstFrameTime,
            get_json_object(get_json_object(line,'$.turndata'), '$.LastFrameTime') LastFrameTime
        from  rtc_du
    )t2
    on t1.connectionid=t2.connectionid
)
insert overwrite table ${APP}.dwd_sdk_su partition (dt = '$do_date')
select \`(r1)?+.+\`,'',
       max(r2) over (partition by connectionid)
from (
         select *,
                row_number() over (partition by connectionid order by r1) r2
         from (
                  select connectionid,
                         city,
                         platform,
                         version,
                         clientype,
                         network,
                         accountid,
                         resolution,
                         vendor,
                         clienttime,
                         get_json_object(turndata_json, '$.ConnTime'),
                         get_json_object(turndata_json, '$.ConnState'),
                         get_json_object(turndata_json, '$.ConnType'),
                         get_json_object(turndata_json, '$.GatherState'),
                         get_json_object(turndata_json, '$.SigState'),
                         get_json_object(turndata_json, '$.IceState'),
                         get_json_object(turndata_json, '$.StreamId'),
                         get_json_object(turndata_json, '$.IsRTC'),
                         get_json_object(turndata_json, '$.EBytes'),
                         get_json_object(turndata_json, '$.HBytes'),
                         get_json_object(turndata_json, '$.PBytes'),
                         get_json_object(turndata_json, '$.RBytes'),
                         get_json_object(turndata_json, '$.Fst1'),
                         get_json_object(turndata_json, '$.Fst2'),
                         get_json_object(turndata_json, '$.Fst3'),
                         get_json_object(turndata_json, '$.Fst4'),
                         get_json_object(turndata_json, '$.LocalIpport'),
                         get_json_object(turndata_json, '$.LocalProto'),
                         get_json_object(turndata_json, '$.LocalType'),
                         get_json_object(turndata_json, '$.RemotePort'),
                         get_json_object(turndata_json, '$.RemoteProto'),
                         get_json_object(turndata_json, '$.RemoteType'),
                         get_json_object(turndata_json, '$.RcvdFrMax'),
                         get_json_object(turndata_json, '$.RcvdFrMin'),
                         get_json_object(turndata_json, '$.RcvdFrMean'),
                         get_json_object(turndata_json, '$.RcvdFrStd'),
                         get_json_object(turndata_json, '$.DecFrMax'),
                         get_json_object(turndata_json, '$.DecFrMin'),
                         get_json_object(turndata_json, '$.DecFrMean'),
                         get_json_object(turndata_json, '$.DecFrStd'),
                         get_json_object(turndata_json, '$.RendFrMax'),
                         get_json_object(turndata_json, '$.RendFrMin'),
                         get_json_object(turndata_json, '$.RendFrMean'),
                         get_json_object(turndata_json, '$.RendFrStd'),
                         get_json_object(turndata_json, '$.RcvdRateMax'),
                         get_json_object(turndata_json, '$.RcvdRateMin'),
                         get_json_object(turndata_json, '$.RcvdRateMean'),
                         get_json_object(turndata_json, '$.RcvdRateStd'),
                         get_json_object(turndata_json, '$.DecStuckTime'),
                         get_json_object(turndata_json, '$.DecStuckCnt'),
                         get_json_object(turndata_json, '$.LDecStuckTime'),
                         get_json_object(turndata_json, '$.LDecStuckCnt'),
                         get_json_object(turndata_json, '$.FirstFrameTime'),
                         get_json_object(turndata_json, '$.LastFrameTime'),
                         logtime,
                         row_number() over (partition by connectionid) r1
                  from (
                           select get_json_object(line, '$.connectionid') connectionid,
                                  get_json_object(line, '$.city')         city,
                                  get_json_object(line, '$.platform')     platform,
                                  get_json_object(line, '$.version')      version,
                                  get_json_object(line, '$.clientype')    clientype,
                                  get_json_object(line, '$.network')      network,
                                  get_json_object(line, '$.accountid')    accountid,
                                  get_json_object(line, '$.resolution')   resolution,
                                  get_json_object(line, '$.vendor')       vendor,
                                  get_json_object(line, '$.clienttime')   clienttime,
                                  get_json_object(line, '$.logtime')      logtime,
                                  turndata_json
                           from (
                                    select line
                                    from rtc_su
                                      where get_json_object(line, '$.turndata') like '[%'
                                      and get_json_object(line, '$.version') < '4.5.0.0'
                                      and get_json_object(line, '$.platform') not in('window_master','window_slave')
                                      and size(split(regexp_replace(
                                                             regexp_replace(get_json_object(line, '$.turndata'), '\\\[|\\\]', ''),
                                                             '\\\}\\\,\\\{', '\\\}\\\;\\\{'), '\\\;')) >
                                          0 
                                    group by line
                                ) t6
                                    lateral view explode(split(
                                            regexp_replace(
                                                    regexp_replace(get_json_object(line, '$.turndata'), '\\\[|\\\]', ''),
                                                    '\\\}\\\,\\\{', '\\\}\\\;\\\{'),
                                            '\\\;')) ods_tuevent_log_tmp as turndata_json
                       ) t7
                  union all
                  select t5.connectionid,
                         t5.city,
                         t5.platform,
                         t5.version,
                         t5.clientype,
                         t5.network,
                         t5.accountid,
                         t5.resolution,
                         t5.vendor,
                         t5.clienttime,
                         get_json_object(t5.turndata_json, '$.ConnTime'),
                         get_json_object(t5.turndata_json, '$.ConnState'),
                         get_json_object(t5.turndata_json, '$.ConnType'),
                         get_json_object(t5.turndata_json, '$.GatherState'),
                         get_json_object(t5.turndata_json, '$.SigState'),
                         get_json_object(t5.turndata_json, '$.IceState'),
                         get_json_object(t5.turndata_json, '$.StreamId'),
                         get_json_object(t5.turndata_json, '$.IsRTC'),
                         get_json_object(t5.turndata_json, '$.EBytes'),
                         get_json_object(t5.turndata_json, '$.HBytes'),
                         get_json_object(t5.turndata_json, '$.PBytes'),
                         get_json_object(t5.turndata_json, '$.RBytes'),
                         get_json_object(t5.turndata_json, '$.Fst1'),
                         get_json_object(t5.turndata_json, '$.Fst2'),
                         get_json_object(t5.turndata_json, '$.Fst3'),
                         get_json_object(t5.turndata_json, '$.Fst4'),
                         get_json_object(t5.turndata_json, '$.LocalIpport'),
                         get_json_object(t5.turndata_json, '$.LocalProto'),
                         get_json_object(t5.turndata_json, '$.LocalType'),
                         get_json_object(t5.turndata_json, '$.RemotePort'),
                         get_json_object(t5.turndata_json, '$.RemoteProto'),
                         get_json_object(t5.turndata_json, '$.RemoteType'),
                         get_json_object(t5.turndata_json, '$.RcvdFrMax'),
                         get_json_object(t5.turndata_json, '$.RcvdFrMin'),
                         get_json_object(t5.turndata_json, '$.RcvdFrMean'),
                         get_json_object(t5.turndata_json, '$.RcvdFrStd'),
                         get_json_object(t5.turndata_json, '$.DecFrMax'),
                         get_json_object(t5.turndata_json, '$.DecFrMin'),
                         get_json_object(t5.turndata_json, '$.DecFrMean'),
                         get_json_object(t5.turndata_json, '$.DecFrStd'),
                         get_json_object(t5.turndata_json, '$.RendFrMax'),
                         get_json_object(t5.turndata_json, '$.RendFrMin'),
                         get_json_object(t5.turndata_json, '$.RendFrMean'),
                         get_json_object(t5.turndata_json, '$.RendFrStd'),
                         get_json_object(t5.turndata_json, '$.RcvdRateMax'),
                         get_json_object(t5.turndata_json, '$.RcvdRateMin'),
                         get_json_object(t5.turndata_json, '$.RcvdRateMean'),
                         get_json_object(t5.turndata_json, '$.RcvdRateStd'),
                         get_json_object(t5.turndata_json, '$.DecStuckTime'),
                         get_json_object(t5.turndata_json, '$.DecStuckCnt'),
                         get_json_object(t5.turndata_json, '$.LDecStuckTime'),
                         get_json_object(t5.turndata_json, '$.LDecStuckCnt'),
                         get_json_object(t5.turndata_json, '$.FirstFrameTime'),
                         get_json_object(t5.turndata_json, '$.LastFrameTime'),
                         t5.logtime,
                         9999
                  from (
                           select get_json_object(line, '$.connectionid') conn_id
                           from (
                                    select line
                                    from rtc_su
                                      where get_json_object(line, '$.turndata') like '[%'
                                      and get_json_object(line, '$.version') < '4.5.0.0'
                                      and size(split(regexp_replace(
                                                             regexp_replace(get_json_object(line, '$.turndata'), '\\\[|\\\]', ''),
                                                             '\\\}\\\,\\\{', '\\\}\\\;\\\{'), '\\\;')) >
                                          0 
                                    group by line
                                ) t1
                       ) t3
                           left join 
                      (
                          select get_json_object(line, '$.connectionid') connectionid,
                                 get_json_object(line, '$.city')         city,
                                 get_json_object(line, '$.platform')     platform,
                                 get_json_object(line, '$.version')      version,
                                 get_json_object(line, '$.clientype')    clientype,
                                 get_json_object(line, '$.network')      network,
                                 get_json_object(line, '$.accountid')    accountid,
                                 get_json_object(line, '$.resolution')   resolution,
                                 get_json_object(line, '$.vendor')       vendor,
                                 get_json_object(line, '$.clienttime')   clienttime,
                                 get_json_object(line, '$.logtime')      logtime,
                                 get_json_object(line, '$.turndata')     turndata_json
                          from (
                                   select line
                                   from rtc_su
                                     where get_json_object(line, '$.version') < '4.5.0.0'
                                     and get_json_object(get_json_object(line, '$.turndata'), '$.IsRTC') = '0'
                                   group by line
                               ) t4
                      ) t5
                                     on t3.conn_id = t5.connectionid
                  where t5.connectionid is not null
              ) t8
     ) t9
union all
select
    *,max(r1) over(partition by connectionid)
from
(
    select  
    connectionid,city,platform,version,clientype,network,accountid,resolution,vendor,clienttime,ConnTime,ConnState,ConnType,GatherState,SigState,IceState,StreamId,IsRTC,EBytes,
    get_json_object(tracks_json,'$.HBytes'),PBytes,
    get_json_object(tracks_json,'$.RBytes'),
    get_json_object(tracks_json,'$.Fst1'),
    get_json_object(tracks_json,'$.Fst2'),
    get_json_object(tracks_json,'$.Fst3'),
    get_json_object(tracks_json,'$.Fst4'),
    LocalIpport,LocalProto,LocalType,RemotePort,RemoteProto,RemoteType,
    get_json_object(tracks_json,'$.RcvdFrMax'),
    get_json_object(tracks_json,'$.RcvdFrMin'),
    get_json_object(tracks_json,'$.RcvdFrMean'),
    get_json_object(tracks_json,'$.RcvdFrStd'),
    get_json_object(tracks_json,'$.DecFrMax'),
    get_json_object(tracks_json,'$.DecFrMin'),
    get_json_object(tracks_json,'$.DecFrMean'),
    get_json_object(tracks_json,'$.DecFrStd'),
    get_json_object(tracks_json,'$.RendFrMax'),
    get_json_object(tracks_json,'$.RendFrMin'),
    get_json_object(tracks_json,'$.RendFrMean'),
    get_json_object(tracks_json,'$.RendFrStd'),
    get_json_object(tracks_json,'$.RcvdRateMax'),
    get_json_object(tracks_json,'$.RcvdRateMin'),
    get_json_object(tracks_json,'$.RcvdRateMean'),
    get_json_object(tracks_json,'$.RcvdRateStd'),
    get_json_object(tracks_json,'$.DecStuckTime'),
    get_json_object(tracks_json,'$.DecStuckCnt'),
    get_json_object(tracks_json,'$.LDecStuckTime'),
    get_json_object(tracks_json,'$.LDecStuckCnt'),
    get_json_object(tracks_json,'$.FirstFrameTime'),
    get_json_object(tracks_json,'$.LastFrameTime'),
    logtime,r1,roll
    from
    (
        select
            connectionid,city,platform,version,clientype,network,accountid,resolution,vendor,clienttime,roll,logtime,
            row_number() over (partition by connectionid) r1,get_json_object(turndata_json,'$.tracks') tr,
            get_json_object(turndata_json, '$.ConnTime') ConnTime,
             get_json_object(turndata_json, '$.ConnState') ConnState,
             get_json_object(turndata_json, '$.ConnType') ConnType,
             get_json_object(turndata_json, '$.GatherState') GatherState,
             get_json_object(turndata_json, '$.SigState') SigState,
             get_json_object(turndata_json, '$.IceState') IceState,
             get_json_object(turndata_json, '$.StreamId') StreamId,
             get_json_object(turndata_json, '$.IsRTC') IsRTC,
             get_json_object(turndata_json, '$.EBytes') EBytes,
             get_json_object(turndata_json, '$.PBytes') PBytes,
             get_json_object(turndata_json, '$.LocalIpport') LocalIpport,
             get_json_object(turndata_json, '$.LocalProto') LocalProto,
             get_json_object(turndata_json, '$.LocalType') LocalType,
             get_json_object(turndata_json, '$.RemotePort') RemotePort,
             get_json_object(turndata_json, '$.RemoteProto') RemoteProto,
             get_json_object(turndata_json, '$.RemoteType') RemoteType
        from
        (
            select
                t0.*
            from
            (
                select
                    get_json_object(line, '$.connectionid') connectionid,
                    get_json_object(line, '$.city')         city,
                    get_json_object(line, '$.platform')     platform,
                    get_json_object(line, '$.version')      version,
                    get_json_object(line, '$.clientype')    clientype,
                    get_json_object(line, '$.network')      network,
                    get_json_object(line, '$.accountid')    accountid,
                    get_json_object(line, '$.resolution')   resolution,
                    get_json_object(line, '$.vendor')       vendor,
                    get_json_object(line, '$.clienttime')   clienttime,
                    get_json_object(get_json_object(line,'$.turndata'),'$.rollback') roll,
                    get_json_object(line, '$.logtime')      logtime,
                    get_json_object(get_json_object(line,'$.turndata'),'$.licode')  licode_json
                from rtc_su
                where (get_json_object(line, '$.version') >= '4.5.0.0' or get_json_object(line, '$.platform') in('window_master','window_slave') )
                and (get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') <> 'AuthFail' or get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') is null)
                and get_json_object(get_json_object(line, '$.turndata'),'$.rollback') in ('0','1')
                and get_json_object(get_json_object(line, '$.turndata'),'$.licode') is not null
            )t0
            left join rtc_etl
            on t0.connectionid = rtc_etl.connid
            where (rtc_etl.su_roll=rtc_etl.du_roll or (su_roll is not null and du_roll is null )) 
        )t1
            lateral view explode(split(
                                    regexp_replace(
                                            regexp_replace(licode_json, '\\\[\\\[|\\\]\\\]', ''),
                                            '\\\}\\\,\\\{\\\"ConnState', '\\\}\\\!!!\\\{\\\"ConnState'),
                                    '\\\!!!')) ods_tuevent_log_tmp as turndata_json
    )t2
        lateral view explode(split(
                                    regexp_replace(
                                            regexp_replace(tr, '\\\[|\\\]', ''),
                                            '\\\}\\\,\\\{\\\"CodecName', '\\\}\\\!!!\\\{\\\"CodecName'),
                                    '\\\!!!')) ods_tuevent_log_tmp as tracks_json
union all
select
    connectionid,
    city,
    platform,
    version,
    clientype,
    network,
    accountid,
    resolution,
    vendor,
    clienttime,
    rtc_etl.ConnTime,
    rtc_etl.ConnState,
    rtc_etl.ConnType,
    rtc_etl.GatherState,
    rtc_etl.SigState,
    rtc_etl.IceState,
    rtc_etl.StreamId,
    rtc_etl.IsRTC,
    rtc_etl.EBytes,
    rtc_etl.HBytes,
    rtc_etl.PBytes,
    rtc_etl.RBytes,
    rtc_etl.Fst1,
    rtc_etl.Fst2,
    rtc_etl.Fst3,
    rtc_etl.Fst4,
    rtc_etl.LocalIpport,
    rtc_etl.LocalProto,
    rtc_etl.LocalType,
    rtc_etl.RemotePort,
    rtc_etl.RemoteProto,
    rtc_etl.RemoteType,
    rtc_etl.RcvdFrMax,
    rtc_etl.RcvdFrMin,
    rtc_etl.RcvdFrMean,
    rtc_etl.RcvdFrStd,
    rtc_etl.DecFrMax,
    rtc_etl.DecFrMin,
    rtc_etl.DecFrMean,
    rtc_etl.DecFrStd,
    rtc_etl.RendFrMax,
    rtc_etl.RendFrMin,
    rtc_etl.RendFrMean,
    rtc_etl.RendFrStd,
    rtc_etl.RcvdRateMax,
    rtc_etl.RcvdRateMin,
    rtc_etl.RcvdRateMean,
    rtc_etl.RcvdRateStd,
    rtc_etl.DecStuckTime,
    rtc_etl.DecStuckCnt,
    rtc_etl.LDecStuckTime,
    rtc_etl.LDecStuckCnt,
    rtc_etl.FirstFrameTime,
    rtc_etl.LastFrameTime,
    logtime,
    9999,
    roll
from
(
    select
        get_json_object(line, '$.connectionid') connectionid,
        get_json_object(line, '$.city')         city,
        get_json_object(line, '$.platform')     platform,
        get_json_object(line, '$.version')      version,
        get_json_object(line, '$.clientype')    clientype,
        get_json_object(line, '$.network')      network,
        get_json_object(line, '$.accountid')    accountid,
        get_json_object(line, '$.resolution')   resolution,
        get_json_object(line, '$.vendor')       vendor,
        get_json_object(line, '$.clienttime')   clienttime,
        get_json_object(line, '$.logtime')      logtime,
        get_json_object(get_json_object(line,'$.turndata'),'$.rollback') roll
    from rtc_su
    where (get_json_object(line, '$.version') >= '4.5.0.0' or get_json_object(line, '$.platform') in('window_master','window_slave') )
    and (get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') <> 'AuthFail' or get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') is null)
    and get_json_object(get_json_object(line, '$.turndata'),'$.rollback') ='1'
)t0
left join rtc_etl
on t0.connectionid = rtc_etl.connid
where (rtc_etl.su_roll=rtc_etl.du_roll or (su_roll is not null and du_roll is null )) 
)t0;"

dwd_licode_sdk="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_licode_sdk partition (dt='$do_date')
select
    get_json_object(line,'$.bus'),
    get_json_object(line,'$.clientype'),
    get_json_object(line,'$.memory'),
    get_json_object(line,'$.bios'),
    get_json_object(line,'$.city'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.tz'),
    get_json_object(line,'$.language'),
    get_json_object(line,'$.resolution'),
    get_json_object(line,'$.mac'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.network'),
    get_json_object(line,'$.accountid'),
    get_json_object(line,'$.province'),
    get_json_object(line,'$.vendor'),
    get_json_object(line,'$.action'),
    get_json_object(get_json_object(line,'$.turndata'),'$.alltime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.begintime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.id'),
    get_json_object(get_json_object(line,'$.turndata'),'$.report_type'),
    get_json_object(get_json_object(line,'$.turndata'),'$.report_version'),
    get_json_object(get_json_object(line,'$.turndata'),'$.version'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.id'),
    get_json_object(line,'$.clienttime'),
    get_json_object(line,'$.events'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.cpu'),
    get_json_object(line,'$.gpu'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.pcid'),
    get_json_object(line,'$.mainboard'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_tuevent_log
where dt='$do_date'
and get_json_object(line,'$.events')='licodesdk'
and get_json_object(line,'$.clientype') in ('client','host')
and get_json_object(get_json_object(line,'$.turndata'),'$.report_type')='sdk';"

dwd_licode_sig="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_licode_sig partition (dt='$do_date')
select
    get_json_object(line,'$.bus'),
    get_json_object(line,'$.clientype'),
    get_json_object(line,'$.memory'),
    get_json_object(line,'$.bios'),
    get_json_object(line,'$.city'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.tz'),
    get_json_object(line,'$.language'),
    get_json_object(line,'$.resolution'),
    get_json_object(line,'$.mac'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.network'),
    get_json_object(line,'$.accountid'),
    get_json_object(line,'$.province'),
    get_json_object(line,'$.vendor'),
    get_json_object(line,'$.action'),
    get_json_object(get_json_object(line,'$.turndata'),'$.begintime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.connected'),
    get_json_object(get_json_object(line,'$.turndata'),'$.connectiontime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.disconnectiontimes'),
    get_json_object(get_json_object(line,'$.turndata'),'$.id'),
    get_json_object(get_json_object(line,'$.turndata'),'$.offlinetime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.onlinetime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.report_type'),
    get_json_object(get_json_object(line,'$.turndata'),'$.sdk_id'),
    get_json_object(get_json_object(line,'$.turndata'),'$.url'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.id'),
    get_json_object(line,'$.clienttime'),
    get_json_object(line,'$.events'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.cpu'),
    get_json_object(line,'$.gpu'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.pcid'),
    get_json_object(line,'$.mainboard'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_tuevent_log
where dt='$do_date'
and get_json_object(line,'$.events')='licodesdk'
and get_json_object(line,'$.clientype') in ('client','host')
and get_json_object(get_json_object(line,'$.turndata'),'$.report_type')='sig';"

dwd_licode_ice="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_licode_ice partition (dt='$do_date')
select
    get_json_object(line,'$.bus'),
    get_json_object(line,'$.clientype'),
    get_json_object(line,'$.memory'),
    get_json_object(line,'$.bios'),
    get_json_object(line,'$.city'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.tz'),
    get_json_object(line,'$.language'),
    get_json_object(line,'$.resolution'),
    get_json_object(line,'$.mac'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.network'),
    get_json_object(line,'$.accountid'),
    get_json_object(line,'$.province'),
    get_json_object(line,'$.vendor'),
    get_json_object(line,'$.action'),
    get_json_object(get_json_object(line,'$.turndata'),'$.begintime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.connected'),
    if(get_json_object(get_json_object(line,'$.turndata'),'$.connected')='1' AND cast(get_json_object(get_json_object(line,'$.turndata'),'$.onlinetime') as bigint) > 5000 AND cast(get_json_object(get_json_object(line,'$.turndata'),'$.onlinetime') as bigint) < 2592000000,'1','0' ),
    get_json_object(get_json_object(line,'$.turndata'),'$.connection_type'),
    get_json_object(get_json_object(line,'$.turndata'),'$.connectiontime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.disconnectiontimes'),
    get_json_object(get_json_object(line,'$.turndata'),'$.id'),
    get_json_object(get_json_object(line,'$.turndata'),'$.offlinetime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.onlinetime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.report_type'),
    get_json_object(get_json_object(line,'$.turndata'),'$.sdk_id'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.id'),
    get_json_object(line,'$.clienttime'),
    get_json_object(line,'$.events'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.cpu'),
    get_json_object(line,'$.gpu'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.pcid'),
    get_json_object(line,'$.mainboard'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_tuevent_log
where dt='$do_date'
and get_json_object(line,'$.events')='licodesdk'
and get_json_object(line,'$.clientype') in ('client','host')
and get_json_object(get_json_object(line,'$.turndata'),'$.report_type')='ice';"


dwd_android_licode_su="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_android_licode_su partition (dt='$do_date')
select
    get_json_object(line,'$.id'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.events'),
    get_json_object(line,'$.action'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.vendor'),
    get_json_object(line,'$.bios'),
    get_json_object(get_json_object(line,'$.turndata'),'$.Abnormal'),
    get_json_object(get_json_object(line,'$.turndata'),'$.ConnTime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.ConnState'),
    get_json_object(get_json_object(line,'$.turndata'),'$.ConnType'),
    get_json_object(get_json_object(line,'$.turndata'),'$.IceState'),
    get_json_object(get_json_object(line,'$.turndata'),'$.FirstFrameTime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.RcvdFrMean'),
    get_json_object(get_json_object(line,'$.turndata'),'$.DecFrMean'),
    get_json_object(get_json_object(line,'$.turndata'),'$.RendFrMean'),
    get_json_object(get_json_object(line,'$.turndata'),'$.EBytes'),
    get_json_object(get_json_object(line,'$.turndata'),'$.HBytes'),
    get_json_object(get_json_object(line,'$.turndata'),'$.PBytes'),
    get_json_object(get_json_object(line,'$.turndata'),'$.RBytes'),
    get_json_object(get_json_object(line,'$.turndata'),'$.Fst1'),
    get_json_object(get_json_object(line,'$.turndata'),'$.Fst2'),
    get_json_object(get_json_object(line,'$.turndata'),'$.Fst3'),
    get_json_object(get_json_object(line,'$.turndata'),'$.Fst4'),
    get_json_object(get_json_object(line,'$.turndata'),'$.MaxDecodeMs'),
    get_json_object(get_json_object(line,'$.turndata'),'$.DecStuckCnt'),
    get_json_object(get_json_object(line,'$.turndata'),'$.DlDecMean'),
    get_json_object(get_json_object(line,'$.turndata'),'$.DecoderImplementation'),
    get_json_object(get_json_object(line,'$.turndata'),'$.abflag'),
    get_json_object(get_json_object(line,'$.turndata'),'$.IsRTC'),
    get_json_object(line, '$.logtime')
from ${APP}.ods_tuevent_log
where dt ='$do_date'
AND get_json_object(line,'$.events') in ('rtc_link_tech','rtc_cycle_tech')
AND get_json_object(line,'$.action')='client';"


dwd_android_state_machine="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_android_state_machine partition (dt='$do_date')
select
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.action'),
    get_json_object(get_json_object(line,'$.turndata'),'$.SubState'),
    get_json_object(get_json_object(line,'$.turndata'),'$.verifyStep'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_tuevent_log
where dt ='$do_date'
and get_json_object(line,'$.events')='state_machine';"

dwd_android_link_start_result="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_android_link_start_result partition (dt='$do_date')
select
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.clientype'),
    get_json_object(line,'$.platform'),
    get_json_object(get_json_object(line,'$.turndata'),'$.devtype'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.action'),
    get_json_object(get_json_object(line,'$.turndata'),'$.result_code'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_tuevent_log
where dt ='$do_date'
and get_json_object(line,'$.events')='rtc_link_tech'
and get_json_object(line,'$.action') in ('link_start','link_result');"

dwd_link_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_link_info partition (dt='$do_date')
select
    get_json_object(line,'$.bus'),
    get_json_object(line,'$.clientype'),
    get_json_object(line,'$.memory'),
    get_json_object(line,'$.bios'),
    get_json_object(line,'$.city'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.tz'),
    get_json_object(line,'$.language'),
    get_json_object(line,'$.resolution'),
    get_json_object(line,'$.mac'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.network'),
    get_json_object(line,'$.accountid'),
    get_json_object(line,'$.province'),
    get_json_object(line,'$.vendor'),
    get_json_object(line,'$.action'),
    get_json_object(line,'$.turndata'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.id'),
    get_json_object(line,'$.clienttime'),
    get_json_object(line,'$.events'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.cpu'),
    get_json_object(line,'$.gpu'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.pcid'),
    get_json_object(line,'$.mainboard'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_tuevent_log
where dt ='$do_date'
and get_json_object(line,'$.events')='link_info';"

dwd_client_section_su="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_client_section_su partition (dt='$do_date')
select
    get_json_object(line,'$.id'),
    get_json_object(line,'$.accountid'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.cpu'),
    get_json_object(line,'$.gpu'),
    get_json_object(line,'$.resolution'),
    get_json_object(line,'$.clienttime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.DlDecMean'),
    get_json_object(get_json_object(line,'$.turndata'),'$.DlEncMean'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_tuevent_log
where dt ='$do_date'
and get_json_object(line,'$.events')='section'
and get_json_object(line,'$.clientype')='client'
and get_json_object(get_json_object(line,'$.turndata'),'$.connect_RTC') is null;"

dwd_client_section_du="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_client_section_du partition (dt='$do_date')
select
    get_json_object(line,'$.id'),
    get_json_object(line,'$.accountid'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.cpu'),
    get_json_object(line,'$.gpu'),
    get_json_object(line,'$.resolution'),
    get_json_object(line,'$.clienttime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.CodecName'),
    get_json_object(get_json_object(line,'$.turndata'),'$.Enc'),
    get_json_object(get_json_object(line,'$.turndata'),'$.EncTime'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_tuevent_log
where dt ='$do_date'
and get_json_object(line,'$.events')='section'
and get_json_object(line,'$.clientype')='host'
and get_json_object(get_json_object(line,'$.turndata'),'$.connect_RTC') is null;"

dwd_licode_ice_connection="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_licode_ice_connection partition (dt='$do_date')
select
    get_json_object(line,'$.clientype'),
    get_json_object(line,'$.memory'),
    get_json_object(line,'$.bios'),
    get_json_object(line,'$.city'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.tz'),
    get_json_object(line,'$.language'),
    get_json_object(line,'$.resolution'),
    get_json_object(line,'$.mac'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.network'),
    get_json_object(line,'$.accountid'),
    get_json_object(line,'$.province'),
    get_json_object(line,'$.vendor'),
    get_json_object(get_json_object(line,'$.turndata'),'$.connectionId'),
    get_json_object(get_json_object(line,'$.turndata'),'$.sdk_version'),
    get_json_object(get_json_object(line,'$.turndata'),'$.connected'),
    get_json_object(get_json_object(line,'$.turndata'),'$.connection_type'),
    get_json_object(get_json_object(line,'$.turndata'),'$.fullConnectionId'),
    get_json_object(get_json_object(line,'$.turndata'),'$.streamInfo'),
    get_json_object(get_json_object(line,'$.turndata'),'$.hasRemoteDescription'),
    get_json_object(get_json_object(line,'$.turndata'),'$.hasLocalDescription'),
    get_json_object(get_json_object(line,'$.turndata'),'$.duration'),
    get_json_object(line,'$.id'),
    get_json_object(line,'$.clienttime'),
    get_json_object(line,'$.events'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.cpu'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_tuevent_log
where dt='$do_date'
and get_json_object(line,'$.events')='licodesdk'
and get_json_object(line,'$.clientype') in ('client','host')
and get_json_object(get_json_object(line,'$.turndata'),'$.report_type')='iceConnectionState';"

dwd_client_tevent="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_client_tevent partition (dt='$do_date')
select
    get_json_object(line,'$.id'),
    get_json_object(line,'$.action'),
    get_json_object(line,'$.errno'),
    get_json_object(line,'$.network'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.province'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.gpu'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.accountid'),
    get_json_object(line,'$.mac'),
    get_json_object(line,'$.resolution'),
    get_json_object(line,'$.language'),
    get_json_object(line,'$.tz'),
    get_json_object(line,'$.bus'),
    get_json_object(line,'$.clientype'),
    get_json_object(line,'$.nettype'),
    get_json_object(line,'$.eventtype'),
    get_json_object(line,'$.mainboard'),
    get_json_object(line,'$.pcid'),
    get_json_object(line,'$.vendor'),
    get_json_object(line,'$.cpu'),
    get_json_object(line,'$.city'),
    get_json_object(line,'$.bios'),
    get_json_object(line,'$.memory'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.clienttime'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_tevent_log
where dt ='$do_date';"

dwd_client_event="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_client_event partition (dt='$do_date')
select
    get_json_object(line,'$.bus'),
    get_json_object(line,'$.memory'),
    get_json_object(line,'$.bios'),
    get_json_object(line,'$.city'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.tz'),
    get_json_object(line,'$.language'),
    get_json_object(line,'$.resolution'),
    get_json_object(line,'$.mac'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.network'),
    get_json_object(line,'$.accountid'),
    get_json_object(line,'$.province'),
    get_json_object(line,'$.vendor'),
    get_json_object(line,'$.ck_url'),
    get_json_object(line,'$.action'),
    get_json_object(line,'$.id'),
    get_json_object(line,'$.clienttime'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.module'),
    get_json_object(line,'$.cpu'),
    get_json_object(line,'$.gpu'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.pcid'),
    get_json_object(line,'$.page'),
    get_json_object(line,'$.mainboard'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_event_log
where dt ='$do_date';"

dwd_client_click="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_client_click partition (dt='$do_date')
select
    get_json_object(line,'$.bus'),
    get_json_object(line,'$.memory'),
    get_json_object(line,'$.bios'),
    get_json_object(line,'$.city'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.tz'),
    get_json_object(line,'$.language'),
    get_json_object(line,'$.resolution'),
    get_json_object(line,'$.mac'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.network'),
    get_json_object(line,'$.accountid'),
    get_json_object(line,'$.province'),
    get_json_object(line,'$.vendor'),
    get_json_object(line,'$.ck_url'),
    get_json_object(line,'$.action'),
    get_json_object(line,'$.module'),
    get_json_object(line,'$.page'),
    get_json_object(line,'$.id'),
    get_json_object(line,'$.clienttime'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.cpu'),
    get_json_object(line,'$.gpu'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.pcid'),
    get_json_object(line,'$.mainboard'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_click_log
where dt ='$do_date';"

dwd_client_connect="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_client_connect partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.sport'),
    get_json_object(line,'$.oncomet'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.connsesion'),
    get_json_object(line,'$.onlinelink'),
    get_json_object(line,'$.runtime'),
    get_json_object(line,'$.uuid'),
    get_json_object(line,'$.guid'),
    get_json_object(line,'$.competing'),
    get_json_object(line,'$.city'),
    get_json_object(line,'$.province'),
    get_json_object(line,'$.line'),
    get_json_object(line,'$.isp'),
    get_json_object(line,'$.client'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.pcname'),
    get_json_object(line,'$.systemtime'),
    get_json_object(line,'$.maclist'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_client_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'client_connect'
and get_json_object(line,'$.labtype') = 'logic';"

dwd_client_closed="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_client_closed partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.client'),
    nvl(get_json_object(line,'$.vsersion'),get_json_object(line,'$.version')),
    get_json_object(line,'$.ontime'),
    get_json_object(line,'$.result'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.oncomet'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_client_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'client_closed'
and get_json_object(line,'$.labtype') = 'logic';"

dwd_web_config_center="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_web_config_center partition (dt='$do_date')
select
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.clientype'),
    get_json_object(line,'$.action'),
    get_json_object(line,'$.log'),
    get_json_object(line,'$.network'),
    get_json_object(line,'$.systemtime'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_client_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='config_center';"

dwd_client_licode_detail="
insert overwrite table ${APP}.dwd_client_licode_detail partition (dt='$do_date')
select
	*,ts - if(lag(ts,1,0) over (partition by connectionid,clientype order by ts)=0,ts,lag(ts,1,0) over (partition by connectionid,clientype order by ts))
from(
select
    connectionid,
    clientype,
    version,
    action,
    ts,
    result
from
(
    select
        connectionid,
        clientype,
	version,
        case
           when connected='0' then concat_ws('_',connection_type,if(split(fullConnection_id,'_')[2]=connectionid,'media','file'),'noconnected')
           when connected='1' then concat_ws('_',connection_type,if(split(fullConnection_id,'_')[2]=connectionid,'media','file'),'connected')
           when connected='2' then concat_ws('_',connection_type,if(split(fullConnection_id,'_')[2]=connectionid,'media','file'),'actionclose')
	   when action='sub_ready' then concat_ws('_',action,lower(conn_type))
        else action
        end action,
        ts,
        result
    from
    (
        select
            get_json_object(line,'$.connectionid') connectionid,
            get_json_object(line,'$.clientype') clientype,
            get_json_object(line, '$.action') action,
	    get_json_object(line, '$.version') version,
            get_json_object(get_json_object(line, '$.turndata'),'$.connected') connected,
            get_json_object(get_json_object(line, '$.turndata'),'$.ts') ts,
            if(get_json_object(line,'$.action')='first_render_frame',get_json_object(get_json_object(line,'$.turndata'),'$.action'),get_json_object(get_json_object(line,'$.turndata'),'$.result')) result,
            get_json_object(get_json_object(line, '$.turndata'),'$.conn_type') conn_type,
            get_json_object(get_json_object(line, '$.turndata'),'$.connection_type') connection_type,
                get_json_object(get_json_object(line, '$.turndata'),'$.fullConnectionId') fullConnection_id
        from ${APP}.ods_tuevent_log
        where dt='$do_date'
        and get_json_object(line,'$.events')='licodesdk'
        and get_json_object(line,'$.connectionid') is not null
        and get_json_object(line,'$.connectionid') != ''
        and (get_json_object(line,'$.action') in ('subscribe','sub_ready','closeLicode','enter_room','first_render_frame','connect','createLicode','publish')
                or (get_json_object(get_json_object(line, '$.turndata'),'$.connected') in ('0','1','2')
                        and get_json_object(get_json_object(line, '$.turndata'),'$.report_type') = 'iceConnectionState')
            )
    )t1
    union all
    select
        conn_id,clientype,version,action,get_json_object(errno,'$.ts'),null
    from ${APP}.dwd_client_tevent
    where dt='$do_date'
    and action='ClientMode_connect'
)t1
)t2;"

case $1 in
    dwd_handshake_sd )
        hive -e "$dwd_handshake_sd"
    ;;
    dwd_authentication_success )
        hive -e "$dwd_authentication_success"
    ;;
    dwd_session_success )
        hive -e "$dwd_session_success"
    ;;
    dwd_new_connect )
        hive -e "$dwd_new_connect"
    ;;
    dwd_client_connect )
        hive -e "$dwd_client_connect"
    ;;
    dwd_client_closed )
        hive -e "$dwd_client_closed"
    ;;
    dwd_user_logout )
        hive -e "$dwd_user_logout"
    ;;
    dwd_handshake_success )
        hive -e "$dwd_handshake_success"
    ;;
    dwd_controller_lost )
        hive -e "$dwd_controller_lost"
    ;;
    dwd_presenter_lost )
        hive -e "$dwd_presenter_lost"
    ;;
    dwd_add_device )
        hive -e "$dwd_add_device"
    ;;
    dwd_delete_device )
        hive -e "$dwd_delete_device"
    ;;
    dwd_edit_device )
        hive -e "$dwd_edit_device"
    ;;
    dwd_add_group )
        hive -e "$dwd_add_group"
    ;;
    dwd_delete_group )
        hive -e "$dwd_delete_group"
    ;;
    dwd_edit_group )
        hive -e "$dwd_edit_group"
    ;;
    dwd_client_tuevent )
        hive -e "$dwd_client_tuevent"
    ;;
    dwd_client_event )
        hive -e "$dwd_client_event"
    ;;
    dwd_client_click )
        hive -e "$dwd_client_click"
    ;;
    dwd_sdk_du )
	hive -e "$dwd_sdk_du"
    ;;
    dwd_sdk_su )
        hive -e "$dwd_sdk_su"
    ;;
    dwd_client_tevent )
        hive -e "$dwd_client_tevent"
    ;;
    dwd_controller_TCP_packet_failure )
        hive -e "$dwd_controller_TCP_packet_failure"
    ;;
    dwd_presenter_TCP_packet_failure )
	hive -e "$dwd_presenter_TCP_packet_failure"
    ;;
    dwd_connection_failure )
	hive -e "$dwd_connection_failure"
    ;;
    dwd_new_user )
        hive -e "$dwd_new_user"
    ;;
    dwd_user_login )
	hive -e "$dwd_user_login"
    ;;
    dwd_wechat_login )
	hive -e "$dwd_wechat_login"
    ;;
    dwd_phone_login )
	hive -e "$dwd_phone_login"
    ;;
    dwd_qrcode_login )
	hive -e "$dwd_qrcode_login"
    ;;
    dwd_auto_login )
	hive -e "$dwd_auto_login"
    ;;
    dwd_login_error )
	hive -e "$dwd_login_error"
    ;;
    dwd_logic_online )
	hive -e "$dwd_logic_online"
    ;;
    dwd_backend_licode_addsubscriber_ready )
	hive -e "$dwd_backend_licode_addsubscriber_ready"
    ;;
    dwd_backend_licode_addPublisher_ready )
	hive -e "$dwd_backend_licode_addPublisher_ready"
    ;;
    dwd_backend_licode_all_ready )
	hive -e "$dwd_backend_licode_all_ready"
    ;;
    dwd_backend_licode_publish_erizo )
	hive -e "$dwd_backend_licode_publish_erizo"
    ;;
    dwd_backend_licode_publish_initializing )
	hive -e "$dwd_backend_licode_publish_initializing"
    ;;
    dwd_backend_licode_subscribe_start )
	hive -e "$dwd_backend_licode_subscribe_start"
    ;;
    dwd_backend_licode_subscribe_p2p )
	hive -e "$dwd_backend_licode_subscribe_p2p"
    ;;
    dwd_web_config_center )
	hive -e "$dwd_web_config_center"
    ;;
    dwd_licode_sdk )
	hive -e "$dwd_licode_sdk"
    ;;
    dwd_licode_sig )
	hive -e "$dwd_licode_sig"
    ;;
    dwd_licode_ice )
	hive -e "$dwd_licode_ice"
    ;;
    dwd_android_licode_su )
	hive -e "$dwd_android_licode_su"
    ;;
    dwd_android_state_machine )
	hive -e "$dwd_android_state_machine"
    ;;
    dwd_android_link_start_result )
	hive -e "$dwd_android_link_start_result"
    ;;
    dwd_connection_closed )
	hive -e "$dwd_connection_closed"
    ;;
    dwd_licode_session_success )
	hive -e "$dwd_licode_session_success"
    ;;
    dwd_comet_break )
	hive -e "$dwd_comet_break"
    ;;
    dwd_media_statistic )
	hive -e "$dwd_media_statistic"
    ;;
    dwd_web_request )
	hive -e "$dwd_web_request"
    ;;
    dwd_link_info )
	hive -e "$dwd_link_info"
    ;;
    dwd_client_section_su )
	hive -e "$dwd_client_section_su"
    ;;
    dwd_client_section_du )
	hive -e "$dwd_client_section_du"
    ;;
    dwd_new_connect_fail )
	hive -e "$dwd_new_connect_fail"
    ;;
    dwd_client_pageview )
	hive -e "$dwd_client_pageview"
    ;;
    dwd_licode_ice_connection )
	hive -e "$dwd_licode_ice_connection"
    ;;
    dwd_backend_licode_newconnect )
	hive -e "$dwd_backend_licode_newconnect"
    ;;
    dwd_backend_licode_reconnect )
	hive -e "$dwd_backend_licode_reconnect"
    ;;
    dwd_backend_licode_sdk_report )
	hive -e "$dwd_backend_licode_sdk_report"
    ;;
    dwd_backend_licode_socket_disconnected )
	hive -e "$dwd_backend_licode_socket_disconnected"
    ;;
    dwd_backend_licode_unpublish )
	hive -e "$dwd_backend_licode_unpublish"
    ;;
    dwd_backend_licode_unsubscribe )
	hive -e "$dwd_backend_licode_unsubscribe"
    ;;
    dwd_backend_licode_subscribe_end )
	hive -e "$dwd_backend_licode_subscribe_end"
    ;;
    dwd_backend_licode_publish_end )
	hive -e "$dwd_backend_licode_publish_end"
    ;;
    dwd_client_licode_detail )
	hive -e "$dwd_client_licode_detail"
    ;;
    cc )
        hive -e "$dwd_backend_licode_addsubscriber_ready$dwd_backend_licode_addPublisher_ready$dwd_backend_licode_all_ready$dwd_backend_licode_publish_erizo$dwd_backend_licode_publish_initializing$dwd_backend_licode_subscribe_start$dwd_backend_licode_subscribe_p2p$dwd_backend_licode_unsubscribe$dwd_backend_licode_unpublish$dwd_backend_licode_sdk_report$dwd_backend_licode_subscribe_end$dwd_backend_licode_publish_end"
    ;;
    all )
        hive -e "$dwd_client_tuevent$dwd_client_event$dwd_client_click$dwd_client_tevent$dwd_client_licode_detail"
    ;;
esac

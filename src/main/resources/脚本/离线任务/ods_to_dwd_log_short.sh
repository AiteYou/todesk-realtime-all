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
    get_json_object(line,'$.labtype'),
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
    get_json_object(line,'$.logtime'),
    get_json_object(line,'$.reconnect')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'newconnect_fail';"

dwd_session_success="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_session_success partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.labtype'),
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
    get_json_object(line,'$.sab'),
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
    get_json_object(line,'$.bid'),
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
    get_json_object(line,'$.source'),
    get_json_object(line,'$.action'),
    get_json_object(line,'$.relayserver'),
    get_json_object(line,'$.relayservername'),
    get_json_object(line,'$.resultmsg'),
    get_json_object(line,'$.sab'),
    get_json_object(line,'$.reconnect'),
    get_json_object(line,'$.id'),
    get_json_object(line,'$.idmax'),
    get_json_object(line,'$.accmax'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.bemail'),
    get_json_object(line,'$.ingroup'),
    get_json_object(line,'$.black'),
    get_json_object(line,'$.regdays'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.runtime'),
    get_json_object(line,'$.out'),
    get_json_object(line,'$.uglobal'),
    get_json_object(line,'$.params'),
    get_json_object(line,'$.newpass'),
    get_json_object(line,'$.black_110'),
    get_json_object(line,'$.sline'),
    get_json_object(line,'$.result'), 
    get_json_object(line,'$.assign'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_now_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'newconnect'
and get_json_object(line,'$.suid') is not null
and get_json_object(line,'$.relayserver') is not null
union all
select
    get_json_object(line,'$.clientid'),
    get_json_object(line,'$.clientip'),
    '',
    get_json_object(line,'$.clientplat'),
    get_json_object(line,'$.clientver'),
    get_json_object(line,'$.userid'),
    '','','','','','','','',
    get_json_object(line,'$.agentid'),
    '',
    '',
    get_json_object(line,'$.agentplat'),
    get_json_object(line,'$.agentver'),
    '','','','','','','','',
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.source'),
    '',
    get_json_object(line,'$.relayserver'),
    get_json_object(line,'$.relayservername'),
    get_json_object(line,'$.resultmsg'),
    '','','','','','','','','','',
    get_json_object(line,'$.labtype'),
    '','','','','','','','','',
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='3'
and get_json_object(line,'$.eventid')='newconnect';"

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
    get_json_object(line,'$.labtype'),
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
     case when get_json_object(line,'$.enterprise')='0' and get_json_object(line,'$.labtype')='logic' then get_json_object(line,'$.srcuser')
	  when get_json_object(line,'$.enterprise')='1' then get_json_object(line,'$.uid')
	  when get_json_object(line,'$.labtype')='daas' then get_json_object(line,'$.userid')
	  else null end,
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.companyid'),
    get_json_object(line,'$.companyname'),
    get_json_object(line,'$.vip'),
    if(get_json_object(line,'$.sip') is not null,get_json_object(line,'$.sip'),get_json_object(line,'$.ip')),
    get_json_object(line,'$.sport'),
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
and (get_json_object(line,'$.logtype')='user_login' );"

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
    if(get_json_object(line,'$.relayserver') is not null,get_json_object(line,'$.relayserver'),get_json_object(line,'$.serv')),
    get_json_object(line,'$.relayservername'),
    get_json_object(line,'$.logtype'),
    get_json_object(line,'$.enterprise'),
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
    get_json_object(line,'$.client'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.next'),
    get_json_object(line,'$.extinfo'),
    get_json_object(line,'$.closetype'),
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
    get_json_object(line,'$.biztype'),
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.isp'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.isP2P'),
    get_json_object(line,'$.isFileTransport'),
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
    get_json_object(get_json_object(line,'$.message'),'$.delay50_100_ms'),
    get_json_object(get_json_object(line,'$.message'),'$.delay100_1000_ms'),
    get_json_object(get_json_object(line,'$.message'),'$.delay1000_ms'),
    get_json_object(get_json_object(line,'$.message'),'$.duration0_10_ms'),
    get_json_object(get_json_object(line,'$.message'),'$.duration10_50_ms'),
    get_json_object(get_json_object(line,'$.message'),'$.duration50_100_ms'),
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

dwd_licode_first_packet_receive="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_licode_first_packet_receive partition (dt='$do_date')
select
    get_json_object(line,'$.streamId'),
    get_json_object(line,'$.connectionId'),
    get_json_object(get_json_object(line,'$.message'),'$.first_packet_ms'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='firstPacketReceive';"

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
    get_json_object(line,'$.ck_url'),
    get_json_object(line,'$.action'),
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
    if(get_json_object(line,'$.events') in( 'link_info','section') ,'',get_json_object(line,'$.turndata')),
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
and get_json_object(line,'$.action') <> '34';"

dwd_sdk_du="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_sdk_du partition (dt = '$do_date')
select
    *,max(r1) over(partition by connectionid)
from
(
    select
        connectionid,city,platform,version,clientype,network,cpu,gpu,accountid,resolution,vendor,clienttime,
        ConnTime,ConnState,ConnType,GatherState,SigState,IceState,StreamId,IsRTC,dcs,
	get_json_object(tracks_json,'$.EBytes'),
        get_json_object(tracks_json,'$.HBytes'),
        get_json_object(tracks_json,'$.PBytes'),
        get_json_object(tracks_json,'$.RBytes'),
        get_json_object(tracks_json,'$.FBytes'),
        get_json_object(tracks_json,'$.Enc'),
        get_json_object(tracks_json,'$.EncErr'),
	get_json_object(tracks_json,'$.EncErrCode'),
        get_json_object(tracks_json,'$.CodecName'),
        LocalIpport,LocalProto,LocalType,RemotePort,RemoteProto,RemoteType,ssping,srpong,srping,sspong,DtlsDur,CpuMax,CpuMin,CpuMean,CpuStd,
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
        LossMax,LossMin,LossMean,LossStd,RlLossMax,RlLossMin,RlLossMean,RlLossStd,
        get_json_object(tracks_json,'$.SendRateMax'),
        get_json_object(tracks_json,'$.SendRateMin'),
        get_json_object(tracks_json,'$.SendRateMean'),
        get_json_object(tracks_json,'$.SendRateStd'),
	BurMax,BurMean,BurMin,NextDelayMax,NextDelayMean,NextDelayMin,FrmRecvRateMax,FrmRecvRateMean,FrmRecvRateMin,FrmDrainRateMax,FrmDrainRateMean,FrmDrainRateMin,
        BadReason,NetReason,BweMax,BweMean,BweMin,UpBweMax,UpBweMean,UpBweMin,FbBweMax,FbBweMean,FbBweMin,FirstFrameTime,Fst1,Fst2,Fst3,Fst4,Fst5,LtB,LtC,LtI,LtR,SSBwe,SSMs,LRttT,LRttM,
            get_json_object(tracks_json,'$.TarFrMax'),
            get_json_object(tracks_json,'$.TarFrMean'),
            get_json_object(tracks_json,'$.TarFrMin'),
            get_json_object(tracks_json,'$.VQT'),EsBweMax,EsBweMean,EsBweMin,OvCtAc,OvCtRe,
	   get_json_object(tracks_json,'$.PSNRMax'),
           get_json_object(tracks_json,'$.PSNRMean'),
           get_json_object(tracks_json,'$.PSNRMin'),
           get_json_object(tracks_json,'$.Resend'),tracks_size,
           AutoCong,BweTh,BrTh,LimitKbpsMax,LimitKbpsMean,LimitKbpsMin,LimitKbpsStd,StartBwe,ActTime,HotSwhQoe,
           get_json_object(tracks_json,'$.FstBwe'),
	   get_json_object(tracks_json,'$.FstTr'),
        logtime,r1,roll
    from
    (
        select
            connectionid,city,platform,version,clientype,network,cpu,gpu,accountid,resolution,vendor,clienttime,roll,logtime,
            row_number() over (partition by connectionid) r1,get_json_object(turndata_json,'$.tracks') tr,size(split(get_json_object(get_json_object(turndata_json,'$.tracks'),'$.[*].CodecName'),',')) tracks_size,
            get_json_object(turndata_json, '$.ConnTime') ConnTime,
            get_json_object(turndata_json, '$.ConnState') ConnState,
            get_json_object(turndata_json, '$.ConnType') ConnType,
            get_json_object(turndata_json, '$.GatherState') GatherState,
            get_json_object(turndata_json, '$.SigState') SigState,
            get_json_object(turndata_json, '$.IceState') IceState,
            get_json_object(turndata_json, '$.StreamId') StreamId,
            get_json_object(turndata_json, '$.IsRTC') IsRTC,
	    get_json_object(turndata_json, '$.DCS') dcs,
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
            split(split(regexp_replace(get_json_object(turndata_json, '$.STUN_INFO'),'\\\[|\\\]',''),'\\\|')[0],'-')[0] ssping,
             split(split(regexp_replace(get_json_object(turndata_json, '$.STUN_INFO'),'\\\[|\\\]',''),'\\\|')[0],'-')[1] srpong,
             split(split(regexp_replace(get_json_object(turndata_json, '$.STUN_INFO'),'\\\[|\\\]',''),'\\\|')[1],'-')[0] srping,
             split(split(regexp_replace(get_json_object(turndata_json, '$.STUN_INFO'),'\\\[|\\\]',''),'\\\|')[1],'-')[1] sspong,
             get_json_object(turndata_json, '$.DtlsDur') DtlsDur,
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
	    get_json_object(turndata_json, '$.BurMax')  BurMax,
            get_json_object(turndata_json, '$.BurMean')  BurMean,
            get_json_object(turndata_json, '$.BurMin')  BurMin   ,
            get_json_object(turndata_json, '$.NextDelayMax')  NextDelayMax,
            get_json_object(turndata_json, '$.NextDelayMean')  NextDelayMean,
            get_json_object(turndata_json, '$.NextDelayMin')  NextDelayMin      ,
            get_json_object(turndata_json, '$.FrmRecvRateMax')  FrmRecvRateMax,
            get_json_object(turndata_json, '$.FrmRecvRateMean')  FrmRecvRateMean,
            get_json_object(turndata_json, '$.FrmRecvRateMin')  FrmRecvRateMin   ,
            get_json_object(turndata_json, '$.FrmDrainRateMax')  FrmDrainRateMax,
            get_json_object(turndata_json, '$.FrmDrainRateMean')  FrmDrainRateMean,
            get_json_object(turndata_json, '$.FrmDrainRateMin')  FrmDrainRateMin,
            get_json_object(turndata_json, '$.BadReason') BadReason,
            get_json_object(turndata_json, '$.NetReason') NetReason,
            get_json_object(turndata_json, '$.BweMax') BweMax,
            get_json_object(turndata_json, '$.BweMean') BweMean,
            get_json_object(turndata_json, '$.BweMin') BweMin,
            get_json_object(turndata_json, '$.UpBweMax') UpBweMax,
            get_json_object(turndata_json, '$.UpBweMean') UpBweMean,
            get_json_object(turndata_json, '$.UpBweMin') UpBweMin,
            get_json_object(turndata_json,'$.FbBweMax') FbBweMax,
            get_json_object(turndata_json,'$.FbBweMean') FbBweMean,
            get_json_object(turndata_json,'$.FbBweMin') FbBweMin,
            get_json_object(turndata_json, '$.FirstFrameTime') FirstFrameTime,
            get_json_object(turndata_json, '$.Fst1') Fst1,
            get_json_object(turndata_json, '$.Fst2') Fst2,
            get_json_object(turndata_json, '$.Fst3') Fst3,
            get_json_object(turndata_json, '$.Fst4') Fst4,
            get_json_object(turndata_json, '$.Fst5') Fst5,
            get_json_object(turndata_json, '$.LtB') LtB,
            get_json_object(turndata_json, '$.LtC') LtC,
            get_json_object(turndata_json, '$.LtI') LtI,
            get_json_object(turndata_json, '$.LtR') LtR,
            get_json_object(turndata_json, '$.SSBwe') SSBwe,
            get_json_object(turndata_json, '$.SSMs') SSMs,
            get_json_object(turndata_json, '$.LRttT') LRttT,
            get_json_object(turndata_json, '$.LRttM') LRttM,
	    get_json_object(turndata_json, '$.EsBweMax') EsBweMax,
            get_json_object(turndata_json, '$.EsBweMean') EsBweMean,
            get_json_object(turndata_json, '$.EsBweMin') EsBweMin,
            get_json_object(turndata_json, '$.OvCtAc') OvCtAc,
            get_json_object(turndata_json, '$.OvCtRe') OvCtRe,
	    get_json_object(turndata_json, '$.AutoCong')  AutoCong,
	get_json_object(turndata_json, '$.BweTh')  BweTh,
	get_json_object(turndata_json, '$.BrTh')  BrTh,
	get_json_object(turndata_json, '$.LimitKbpsMax')  LimitKbpsMax,
	get_json_object(turndata_json, '$.LimitKbpsMean')  LimitKbpsMean,
	get_json_object(turndata_json, '$.LimitKbpsMin')  LimitKbpsMin,
	get_json_object(turndata_json, '$.LimitKbpsStd')  LimitKbpsStd,
	get_json_object(turndata_json, '$.StartBwe')  StartBwe,
	get_json_object(turndata_json, '$.ActTime')  ActTime,
	get_json_object(turndata_json, '$.HotSwhQoe')  HotSwhQoe
        from
        (
            select
                get_json_object(line, '$.connectionid') connectionid,
                get_json_object(line, '$.city')         city,
                get_json_object(line, '$.platform')     platform,
                get_json_object(line, '$.version')      version,
                get_json_object(line, '$.clientype')    clientype,
                get_json_object(line, '$.network')      network,
	        get_json_object(line, '$.cpu')      cpu,
        	get_json_object(line, '$.gpu')      gpu,
                get_json_object(line, '$.accountid')    accountid,
                get_json_object(line, '$.resolution')   resolution,
                get_json_object(line, '$.vendor')       vendor,
                get_json_object(line, '$.clienttime')   clienttime,
                get_json_object(get_json_object(line,'$.turndata'),'$.rollback') roll,
                get_json_object(line, '$.logtime')      logtime,
                get_json_object(get_json_object(line,'$.turndata'),'$.licode')  licode_json
                from ${APP}.ods_tuevent_log
                where dt = '$do_date'
                and get_json_object(line, '$.events') = 'link_info'
                and get_json_object(line, '$.clientype') = 'host'
                and get_json_object(line, '$.connectionid') <> ''
                and (get_json_object(line, '$.version') >= '4.5.0.0' or get_json_object(line, '$.platform') ='windows_slave' )
                and (get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') <> 'AuthFail' or get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') is null)
                
                and get_json_object(get_json_object(line, '$.turndata'),'$.licode') is not null
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
        get_json_object(line, '$.connectionid') connectionid,
        get_json_object(line, '$.city')         city,
        get_json_object(line, '$.platform')     platform,
        get_json_object(line, '$.version')      version,
        get_json_object(line, '$.clientype')    clientype,
        get_json_object(line, '$.network')      network,
	        get_json_object(line, '$.cpu')      cpu,
        get_json_object(line, '$.gpu')      gpu,
        get_json_object(line, '$.accountid')    accountid,
        get_json_object(line, '$.resolution')   resolution,
        get_json_object(line, '$.vendor')       vendor,
        get_json_object(line, '$.clienttime')   clienttime,
        get_json_object(get_json_object(line,'$.turndata'), '$.ConnTime') ConnTime,
        get_json_object(get_json_object(line,'$.turndata'), '$.ConnState') ConnState,
        get_json_object(get_json_object(line,'$.turndata'), '$.ConnType') ConnType,
        get_json_object(get_json_object(line,'$.turndata'), '$.GatherState') GatherState,
        get_json_object(get_json_object(line,'$.turndata'), '$.SigState') SigState,
        get_json_object(get_json_object(line,'$.turndata'), '$.IceState') IceState,
        get_json_object(get_json_object(line,'$.turndata'), '$.StreamId') StreamId,
        get_json_object(get_json_object(line,'$.turndata'), '$.IsRTC') IsRTC,
	get_json_object(get_json_object(line,'$.turndata'), '$.DCS') ,
        get_json_object(get_json_object(line,'$.turndata'), '$.EBytes') EBytes,
        get_json_object(get_json_object(line,'$.turndata'), '$.HBytes') HBytes,
        get_json_object(get_json_object(line,'$.turndata'), '$.PBytes') PBytes,
        get_json_object(get_json_object(line,'$.turndata'), '$.RBytes') RBytes,
	get_json_object(get_json_object(line,'$.turndata'), '$.FBytes') FBytes,
        get_json_object(get_json_object(line,'$.turndata'), '$.Enc') Enc,
        get_json_object(get_json_object(line,'$.turndata'), '$.EncErr') ,
	get_json_object(get_json_object(line,'$.turndata'), '$.EncErrCode'),
        get_json_object(get_json_object(line,'$.turndata'), '$.CodecName') CodecName,
        get_json_object(get_json_object(line,'$.turndata'), '$.LocalIpport') LocalIpport,
        get_json_object(get_json_object(line,'$.turndata'), '$.LocalProto') LocalProto,
        get_json_object(get_json_object(line,'$.turndata'), '$.LocalType') LocalType,
        get_json_object(get_json_object(line,'$.turndata'), '$.RemotePort') RemotePort,
        get_json_object(get_json_object(line,'$.turndata'), '$.RemoteProto') RemoteProto,
        get_json_object(get_json_object(line,'$.turndata'), '$.RemoteType') RemoteType,'','','','','',
        get_json_object(get_json_object(line,'$.turndata'), '$.CpuMax') CpuMax,
        get_json_object(get_json_object(line,'$.turndata'), '$.CpuMin') CpuMin,
        get_json_object(get_json_object(line,'$.turndata'), '$.CpuMean') CpuMean,
        get_json_object(get_json_object(line,'$.turndata'), '$.CpuStd') CpuStd,
        get_json_object(get_json_object(line,'$.turndata'), '$.CapFrMax') CapFrMax,
        get_json_object(get_json_object(line,'$.turndata'), '$.CapFrMin') CapFrMin,
        get_json_object(get_json_object(line,'$.turndata'), '$.CapFrMean') CapFrMean,
        get_json_object(get_json_object(line,'$.turndata'), '$.CapFrStd') CapFrStd,
        get_json_object(get_json_object(line,'$.turndata'), '$.QpMax') QpMax,
        get_json_object(get_json_object(line,'$.turndata'), '$.QpMin') QpMin,
        get_json_object(get_json_object(line,'$.turndata'), '$.QpMean') QpMean,
        get_json_object(get_json_object(line,'$.turndata'), '$.QpStd') QpStd,
        get_json_object(get_json_object(line,'$.turndata'), '$.SendFrMax') SendFrMax,
        get_json_object(get_json_object(line,'$.turndata'), '$.SendFrMin') SendFrMin,
        get_json_object(get_json_object(line,'$.turndata'), '$.SendFrMean') SendFrMean,
        get_json_object(get_json_object(line,'$.turndata'), '$.SendFrStd') SendFrStd,
        get_json_object(get_json_object(line,'$.turndata'), '$.TarRateMax') TarRateMax,
        get_json_object(get_json_object(line,'$.turndata'), '$.TarRateMin') TarRateMin,
        get_json_object(get_json_object(line,'$.turndata'), '$.TarRateMean') TarRateMean,
        get_json_object(get_json_object(line,'$.turndata'), '$.TarRateStd') TarRateStd,
        get_json_object(get_json_object(line,'$.turndata'), '$.ActRateMax') ActRateMax,
        get_json_object(get_json_object(line,'$.turndata'), '$.ActRateMin') ActRateMin,
        get_json_object(get_json_object(line,'$.turndata'), '$.ActRateMean') ActRateMean,
        get_json_object(get_json_object(line,'$.turndata'), '$.ActRateStd') ActRateStd,
        get_json_object(get_json_object(line,'$.turndata'), '$.RttMax') RttMax,
        get_json_object(get_json_object(line,'$.turndata'), '$.RttMin') RttMin,
        get_json_object(get_json_object(line,'$.turndata'), '$.RttMean') RttMean,
        get_json_object(get_json_object(line,'$.turndata'), '$.RttStd') RttStd,
        get_json_object(get_json_object(line,'$.turndata'), '$.LossMax') LossMax,
        get_json_object(get_json_object(line,'$.turndata'), '$.LossMin') LossMin,
        get_json_object(get_json_object(line,'$.turndata'), '$.LossMean') LossMean,
        get_json_object(get_json_object(line,'$.turndata'), '$.LossStd') LossStd,
        get_json_object(get_json_object(line,'$.turndata'), '$.RlLossMax') RlLossMax,
        get_json_object(get_json_object(line,'$.turndata'), '$.RlLossMin') RlLossMin,
        get_json_object(get_json_object(line,'$.turndata'), '$.RlLossMean') RlLossMean,
        get_json_object(get_json_object(line,'$.turndata'), '$.RlLossStd') RlLossStd,
        get_json_object(get_json_object(line,'$.turndata'), '$.SendRateMax') SendRateMax,
        get_json_object(get_json_object(line,'$.turndata'), '$.SendRateMin') SendRateMin,
        get_json_object(get_json_object(line,'$.turndata'), '$.SendRateMean') SendRateMean,
        get_json_object(get_json_object(line,'$.turndata'), '$.SendRateStd') SendRateStd,
        get_json_object(get_json_object(line,'$.turndata'), '$.BurMax'),
        get_json_object(get_json_object(line,'$.turndata'), '$.BurMean'),
        get_json_object(get_json_object(line,'$.turndata'), '$.BurMin'),
        get_json_object(get_json_object(line,'$.turndata'), '$.NextDelayMax'),
        get_json_object(get_json_object(line,'$.turndata'), '$.NextDelayMean'),
        get_json_object(get_json_object(line,'$.turndata'), '$.NextDelayMin'),
        get_json_object(get_json_object(line,'$.turndata'), '$.FrmRecvRateMax'),
        get_json_object(get_json_object(line,'$.turndata'), '$.FrmRecvRateMean'),
        get_json_object(get_json_object(line,'$.turndata'), '$.FrmRecvRateMin'),
        get_json_object(get_json_object(line,'$.turndata'), '$.FrmDrainRateMax'),
        get_json_object(get_json_object(line,'$.turndata'), '$.FrmDrainRateMean'),
        get_json_object(get_json_object(line,'$.turndata'), '$.FrmDrainRateMin'),
        get_json_object(get_json_object(line,'$.turndata'), '$.BadReason')  BadReason,
        get_json_object(get_json_object(line,'$.turndata'), '$.NetReason')  NetReason,
        get_json_object(get_json_object(line,'$.turndata'), '$.BweMax')  BweMax,
        get_json_object(get_json_object(line,'$.turndata'), '$.BweMean')  BweMean,
        get_json_object(get_json_object(line,'$.turndata'), '$.BweMin')  BweMin,
        get_json_object(get_json_object(line,'$.turndata'), '$.UpBweMax')  UpBweMax,
        get_json_object(get_json_object(line,'$.turndata'), '$.UpBweMean')  UpBweMean,
        get_json_object(get_json_object(line,'$.turndata'), '$.UpBweMin')  UpBweMin,
        get_json_object(get_json_object(line,'$.turndata'), '$.UpBweMax')  FbBweMax,
        get_json_object(get_json_object(line,'$.turndata'), '$.UpBweMean')  FbBweMean,
        get_json_object(get_json_object(line,'$.turndata'), '$.UpBweMin')  FbBweMin,
        get_json_object(get_json_object(line,'$.turndata'), '$.FirstFrameTime')  FirstFrameTime,
        get_json_object(get_json_object(line,'$.turndata'), '$.Fst1')  Fst1,
        get_json_object(get_json_object(line,'$.turndata'), '$.Fst2')  Fst2,
        get_json_object(get_json_object(line,'$.turndata'), '$.Fst3')  Fst3,
        get_json_object(get_json_object(line,'$.turndata'), '$.Fst4')  Fst4,
        get_json_object(get_json_object(line,'$.turndata'), '$.Fst5')  Fst5,
        get_json_object(get_json_object(line,'$.turndata'), '$.LtB')  LtB,
        get_json_object(get_json_object(line,'$.turndata'), '$.LtC')  LtC,
        get_json_object(get_json_object(line,'$.turndata'), '$.LtI')  LtI,
        get_json_object(get_json_object(line,'$.turndata'), '$.LtR')  LtR,
        get_json_object(get_json_object(line,'$.turndata'), '$.SSBwe')  SSBwe,
        get_json_object(get_json_object(line,'$.turndata'), '$.SSMs')  SSMs,
        get_json_object(get_json_object(line,'$.turndata'), '$.LRttT')  LRttT,
        get_json_object(get_json_object(line,'$.turndata'), '$.LRttM')  LRttM,
        get_json_object(get_json_object(line,'$.turndata'), '$.TarFrMax')  TarFrMax,
        get_json_object(get_json_object(line,'$.turndata'), '$.TarFrMean')  TarFrMean,
        get_json_object(get_json_object(line,'$.turndata'), '$.TarFrMin')  TarFrMin,
        get_json_object(get_json_object(line,'$.turndata'), '$.VQT')  VQT,
	get_json_object(get_json_object(line,'$.turndata'), '$.EsBweMax'),
        get_json_object(get_json_object(line,'$.turndata'), '$.EsBweMean'),
        get_json_object(get_json_object(line,'$.turndata'), '$.EsBweMin'),
        get_json_object(get_json_object(line,'$.turndata'), '$.OvCtAc'),
        get_json_object(get_json_object(line,'$.turndata'), '$.OvCtRe'),
        get_json_object(get_json_object(line,'$.turndata'), '$.PSNRMax'),
        get_json_object(get_json_object(line,'$.turndata'), '$.PSNRMean'),
        get_json_object(get_json_object(line,'$.turndata'), '$.PSNRMin'),
        get_json_object(get_json_object(line,'$.turndata'), '$.Resend'),-1,
        get_json_object(get_json_object(line,'$.turndata'), '$.AutoCong'),
get_json_object(get_json_object(line,'$.turndata'), '$.BweTh'),
get_json_object(get_json_object(line,'$.turndata'), '$.BrTh'),
get_json_object(get_json_object(line,'$.turndata'), '$.LimitKbpsMax'),
get_json_object(get_json_object(line,'$.turndata'), '$.LimitKbpsMean'),
get_json_object(get_json_object(line,'$.turndata'), '$.LimitKbpsMin'),
get_json_object(get_json_object(line,'$.turndata'), '$.LimitKbpsStd'),
get_json_object(get_json_object(line,'$.turndata'), '$.StartBwe'),
get_json_object(get_json_object(line,'$.turndata'), '$.ActTime'),
get_json_object(get_json_object(line,'$.turndata'), '$.HotSwhQoe'),
get_json_object(get_json_object(line,'$.turndata'), '$.FstBwe'),
get_json_object(get_json_object(line,'$.turndata'), '$.FstTr'),
        get_json_object(line, '$.logtime') logtime,9999,
        get_json_object(get_json_object(line, '$.turndata'),'$.rollback') roll
    from ${APP}.ods_tuevent_log
    where dt = '$do_date'
    and get_json_object(line, '$.events') = 'link_info'
    and get_json_object(line, '$.clientype') = 'client' 
    and get_json_object(line, '$.connectionid') <> ''
    and get_json_object(get_json_object(line,'$.turndata'), '$.ActRateMax') is not null
    and (get_json_object(line, '$.version') >= '4.5.0.0' or get_json_object(line, '$.platform') ='windows_master' )
    and (get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') <> 'AuthFail' or get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') is null)
    and get_json_object(get_json_object(line, '$.turndata'),'$.rollback') ='1'
)t1
union all
select
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.city'),
    get_json_object(line,'$.agentplat'),
    get_json_object(line,'$.agentver'),
    get_json_object(line,'$.source'),
    get_json_object(line,'$.network'),
    get_json_object(line, '$.cpu')   ,
    get_json_object(line, '$.gpu')   ,
    get_json_object(line,'$.userid'),
    get_json_object(line,'$.resolution'),
    get_json_object(line,'$.vendor'),
    get_json_object(line,'$.clienttime'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.ConnTime'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.ConnState'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.ConnType'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.GatherState'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.SigState'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.IceState'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.StreamId'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.IsRTC'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.DCS'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.EBytes') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.HBytes') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.PBytes') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.RBytes') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.FBytes') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.Enc') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.EncErr') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.EncErrCode') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.CodecName') ,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.LocalIpport') ,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.LocalProto') ,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.LocalType') ,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.RemotePort') ,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.RemoteProto') ,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.RemoteType') ,
    '','','','',
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.DtlsDur') ,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.CpuMax') ,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.CpuMin') ,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.CpuMean') ,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.CpuStd') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.CapFrMax'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.CapFrMin'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.CapFrMean'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.CapFrStd'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.QpMax'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.QpMin'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.QpMean'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.QpStd'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.SendFrMax'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.SendFrMin'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.SendFrMean'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.SendFrStd'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.TarRateMax') TarRateMax,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.TarRateMin') TarRateMin,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.TarRateMean') TarRateMean,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.TarRateStd') TarRateStd,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.ActRateMax') ActRateMax,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.ActRateMin') ActRateMin,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.ActRateMean') ActRateMean,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.ActRateStd') ActRateStd,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.RttMax') RttMax,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.RttMin') RttMin,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.RttMean') RttMean,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.RttStd') RttStd,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.LossMax') LossMax,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.LossMin') LossMin,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.LossMean') LossMean,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.LossStd') LossStd,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.RlLossMax') RlLossMax,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.RlLossMin') RlLossMin,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.RlLossMean') RlLossMean,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.RlLossStd') RlLossStd,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.SendRateMax'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.SendRateMin'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.SendRateMean'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.SendRateStd'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.BurMax')  BurMax,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.BurMean')  BurMean,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.BurMin')  BurMin   ,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.NextDelayMax')  NextDelayMax,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.NextDelayMean')  NextDelayMean,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.NextDelayMin')  NextDelayMin      ,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.FrmRecvRateMax')  FrmRecvRateMax,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.FrmRecvRateMean')  FrmRecvRateMean,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.FrmRecvRateMin')  FrmRecvRateMin   ,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.FrmDrainRateMax')  FrmDrainRateMax,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.FrmDrainRateMean')  FrmDrainRateMean,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.FrmDrainRateMin')  FrmDrainRateMin,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.BadReason') BadReason,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.NetReason') NetReason,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.BweMax') BweMax,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.BweMean') BweMean,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.BweMin') BweMin,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.UpBweMax') UpBweMax,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.UpBweMean') UpBweMean,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.UpBweMin') UpBweMin,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.FbBweMax') FbBweMax,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.FbBweMean') FbBweMean,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.FbBweMin') FbBweMin,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.FirstFrameTime') FirstFrameTime,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.Fst1') Fst1,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.Fst2') Fst2,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.Fst3') Fst3,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.Fst4') Fst4,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.Fst5') Fst5,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.LtB') LtB,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.LtC') LtC,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.LtI') LtI,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.LtR') LtR,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.SSBwe') SSBwe,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.SSMs') SSMs,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.LRttT') LRttT,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.LRttM') LRttM,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.TarFrMax'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.TarFrMean'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.TarFrMin'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.VQT'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.EsBweMax') EsBweMax,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.EsBweMean') EsBweMean,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.EsBweMin') EsBweMin,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.OvCtAc') OvCtAc,
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.OvCtRe') OvCtRe,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.PSNRMax'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.PSNRMean'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.PSNRMin'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.Resend'),
    1,
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'), '$.AutoCong'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'), '$.BweTh'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'), '$.BrTh'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'), '$.LimitKbpsMax'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'), '$.LimitKbpsMean'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'), '$.LimitKbpsMin'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'), '$.LimitKbpsStd'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'), '$.StartBwe'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'), '$.ActTime'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'), '$.HotSwhQoe'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'), '$.FstBwe'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'), '$.FstTr'),
    get_json_object(get_json_object(line,'$.turndata'),'$.logtime'),
    1,
    get_json_object(get_json_object(line,'$.turndata'),'$.rollback'),
    1
from ${APP}.ods_other_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='3'
and get_json_object(line,'$.eventid') = 'link_info'
and get_json_object(line,'$.source') ='1';"

dwd_sdk_su="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_sdk_su partition (dt = '$do_date')
select
    *,max(r1) over(partition by connectionid)
from
(
    select
        connectionid,city,platform,version,clientype,network,cpu,gpu,accountid,resolution,vendor,clienttime,ConnTime,ConnState,ConnType,GatherState,SigState,IceState,StreamId,IsRTC,EBytes,
        get_json_object(tracks_json,'$.HBytes'),PBytes,
        get_json_object(tracks_json,'$.RBytes'),
        get_json_object(tracks_json,'$.Dec'),
        get_json_object(tracks_json,'$.DecErr'),
        get_json_object(tracks_json,'$.DecErrCode'),
        get_json_object(tracks_json,'$.CodecName'),
	dcs,
        Fst1,
        get_json_object(tracks_json,'$.Fst2'),
        get_json_object(tracks_json,'$.Fst3'),
        get_json_object(tracks_json,'$.Fst4'),
        LocalIpport,LocalProto,LocalType,RemotePort,RemoteProto,RemoteType,ssping,srpong,srping,sspong,DtlsDur,
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
        get_json_object(tracks_json,'$.ReStoreCnt') ReStoreCnt,
        get_json_object(tracks_json,'$.VQp'),
	get_json_object(tracks_json,'$.VRtc'),
	get_json_object(tracks_json,'$.VStk'),
	get_json_object(tracks_json,'$.VCap'),
	get_json_object(tracks_json,'$.VEnc'),
	get_json_object(tracks_json,'$.VPac'),
	get_json_object(tracks_json,'$.VRec'),
	get_json_object(tracks_json,'$.VJit'),
        get_json_object(tracks_json,'$.VDec'),
	get_json_object(tracks_json,'$.VRtt'),
	get_json_object(tracks_json,'$.AcIdx'),
	get_json_object(tracks_json,'$.ActTime'),
	get_json_object(tracks_json,'$.HxFrTime'),
	get_json_object(tracks_json,'$.Stks'),
	get_json_object(tracks_json,'$.FrameCnts'),
        logtime,r1,roll
    from
    (
        select
            connectionid,city,platform,version,clientype,network,cpu,gpu,accountid,resolution,vendor,clienttime,roll,logtime,
            row_number() over (partition by connectionid) r1,get_json_object(turndata_json,'$.tracks') tr,
	    get_json_object(turndata_json, '$.DCS') dcs,
            get_json_object(turndata_json, '$.Fst1') Fst1,
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
             get_json_object(turndata_json, '$.RemoteType') RemoteType,
             split(split(regexp_replace(get_json_object(turndata_json, '$.STUN_INFO'),'\\\[|\\\]',''),'\\\|')[0],'-')[0] ssping,
             split(split(regexp_replace(get_json_object(turndata_json, '$.STUN_INFO'),'\\\[|\\\]',''),'\\\|')[0],'-')[1] srpong,
             split(split(regexp_replace(get_json_object(turndata_json, '$.STUN_INFO'),'\\\[|\\\]',''),'\\\|')[1],'-')[0] srping,
             split(split(regexp_replace(get_json_object(turndata_json, '$.STUN_INFO'),'\\\[|\\\]',''),'\\\|')[1],'-')[1] sspong,
             get_json_object(turndata_json, '$.DtlsDur') DtlsDur
        from
        (
            select
                get_json_object(line, '$.connectionid') connectionid,
                get_json_object(line, '$.city')         city,
                get_json_object(line, '$.platform')     platform,
                get_json_object(line, '$.version')      version,
                get_json_object(line, '$.clientype')    clientype,
                get_json_object(line, '$.network')      network,
                get_json_object(line, '$.cpu')      cpu,
        get_json_object(line, '$.gpu')      gpu,
                get_json_object(line, '$.accountid')    accountid,
                get_json_object(line, '$.resolution')   resolution,
                get_json_object(line, '$.vendor')       vendor,
                get_json_object(line, '$.clienttime')   clienttime,
                get_json_object(get_json_object(line,'$.turndata'),'$.rollback') roll,
                get_json_object(line, '$.logtime')      logtime,
                get_json_object(get_json_object(line,'$.turndata'),'$.licode')  licode_json
            from ${APP}.ods_tuevent_log
            where dt = '$do_date'
            and get_json_object(line, '$.events') = 'link_info'
            and get_json_object(line, '$.clientype') = 'client'
            and get_json_object(line, '$.connectionid') <> ''
            and (get_json_object(line, '$.version') >= '4.5.0.0' or get_json_object(line, '$.platform') in('windows_master','macOS_master') )
            and (get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') <> 'AuthFail' or get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') is null)
            and get_json_object(get_json_object(line, '$.turndata'),'$.rollback') ='0'
            and get_json_object(get_json_object(line, '$.turndata'),'$.licode') is not null
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
        get_json_object(line,'$.connectionid'),
        get_json_object(line,'$.city'),
        get_json_object(line,'$.platform'),
        get_json_object(line,'$.version'),
        get_json_object(line,'$.clientype'),
        get_json_object(line,'$.network'),
        get_json_object(line, '$.cpu')      cpu,
        get_json_object(line, '$.gpu')      gpu,
        get_json_object(line,'$.accountid'),
        get_json_object(line,'$.resolution'),
        get_json_object(line,'$.vendor'),
        get_json_object(line,'$.clienttime'),
        get_json_object(get_json_object(line,'$.turndata'),'$.ConnTime'),
        get_json_object(get_json_object(line,'$.turndata'),'$.ConnState'),
        get_json_object(get_json_object(line,'$.turndata'),'$.ConnType'),
        get_json_object(get_json_object(line,'$.turndata'),'$.GatherState'),
        get_json_object(get_json_object(line,'$.turndata'),'$.SigState'),
        get_json_object(get_json_object(line,'$.turndata'),'$.IceState'),
        get_json_object(get_json_object(line,'$.turndata'),'$.StreamId'),
        get_json_object(get_json_object(line,'$.turndata'),'$.IsRTC'),
        get_json_object(get_json_object(line,'$.turndata'),'$.EBytes'),
        get_json_object(get_json_object(line,'$.turndata'),'$.HBytes'),
        get_json_object(get_json_object(line,'$.turndata'),'$.PBytes'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RBytes'),
        get_json_object(get_json_object(line,'$.turndata'),'$.Dec'),
        get_json_object(get_json_object(line,'$.turndata'), '$.DecErr') ,
        get_json_object(get_json_object(line,'$.turndata'), '$.EecErrCode'),
        get_json_object(get_json_object(line,'$.turndata'),'$.CodecName'),
        get_json_object(get_json_object(line,'$.turndata'),'$.DCS'),
        get_json_object(get_json_object(line,'$.turndata'),'$.Fst1'),
        get_json_object(get_json_object(line,'$.turndata'),'$.Fst2'),
        get_json_object(get_json_object(line,'$.turndata'),'$.Fst3'),
        get_json_object(get_json_object(line,'$.turndata'),'$.Fst4'),
        get_json_object(get_json_object(line,'$.turndata'),'$.LocalIpport'),
        get_json_object(get_json_object(line,'$.turndata'),'$.LocalProto'),
        get_json_object(get_json_object(line,'$.turndata'),'$.LocalType'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RemotePort'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RemoteProto'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RemoteType'),
        get_json_object(get_json_object(line,'$.turndata'),'$.ssping'),
        get_json_object(get_json_object(line,'$.turndata'),'$.srpong'),
        get_json_object(get_json_object(line,'$.turndata'),'$.srping'),
        get_json_object(get_json_object(line,'$.turndata'),'$.sspong'),
        get_json_object(get_json_object(line,'$.turndata'),'$.DtlsDur'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RcvdFrMax'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RcvdFrMin'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RcvdFrMean'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RcvdFrStd'),
        get_json_object(get_json_object(line,'$.turndata'),'$.DecFrMax'),
        get_json_object(get_json_object(line,'$.turndata'),'$.DecFrMin'),
        get_json_object(get_json_object(line,'$.turndata'),'$.DecFrMean'),
        get_json_object(get_json_object(line,'$.turndata'),'$.DecFrStd'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RendFrMax'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RendFrMin'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RendFrMean'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RendFrStd'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RcvdRateMax'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RcvdRateMin'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RcvdRateMean'),
        get_json_object(get_json_object(line,'$.turndata'),'$.RcvdRateStd'),
        get_json_object(get_json_object(line,'$.turndata'),'$.DecStuckTime'),
        get_json_object(get_json_object(line,'$.turndata'),'$.DecStuckCnt'),
        get_json_object(get_json_object(line,'$.turndata'),'$.LDecStuckTime'),
        get_json_object(get_json_object(line,'$.turndata'),'$.LDecStuckCnt'),
        get_json_object(get_json_object(line,'$.turndata'),'$.FirstFrameTime'),
        get_json_object(get_json_object(line,'$.turndata'),'$.LastFrameTime'),
        get_json_object(get_json_object(line,'$.turndata'),'$.ReStoreCnt') ReStoreCnt,
	get_json_object(get_json_object(line,'$.turndata'),'$.VQp'),
	get_json_object(get_json_object(line,'$.turndata'),'$.VRtc'),
	get_json_object(get_json_object(line,'$.turndata'),'$.VStk'),
	get_json_object(get_json_object(line,'$.turndata'),'$.VCap'),
	get_json_object(get_json_object(line,'$.turndata'),'$.VEnc'),
	get_json_object(get_json_object(line,'$.turndata'),'$.VPac'),
	get_json_object(get_json_object(line,'$.turndata'),'$.VRec'),
	get_json_object(get_json_object(line,'$.turndata'),'$.VJit'),
        get_json_object(get_json_object(line,'$.turndata'),'$.VDec'),
	get_json_object(get_json_object(line,'$.turndata'),'$.VRtt'),
	get_json_object(get_json_object(line,'$.turndata'),'$.AcIdx'),
get_json_object(get_json_object(line,'$.turndata'),'$.ActTime'),
get_json_object(get_json_object(line,'$.turndata'),'$.HxFrTime'),
get_json_object(get_json_object(line,'$.turndata'),'$.Stks'),
get_json_object(get_json_object(line,'$.turndata'),'$.FrameCnts'),
        get_json_object(line,'$.logtime'),9999,get_json_object(get_json_object(line, '$.turndata'),'$.rollback')
    from ${APP}.ods_tuevent_log
    where dt = '$do_date'
    and get_json_object(line, '$.events') = 'link_info'
    and get_json_object(line, '$.clientype') = 'client'
    and get_json_object(line, '$.connectionid') <> ''
    and  (get_json_object(line, '$.version') >= '4.5.0.0' or get_json_object(line, '$.platform') in('windows_master','macOS_master'))
    and (get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') <> 'AuthFail' or get_json_object(get_json_object(line, '$.turndata'),'$.errinfo') is null)
    and get_json_object(get_json_object(line, '$.turndata'),'$.rollback') ='1'
    and get_json_object(get_json_object(line, '$.turndata'),'$.licode') is null
)tt
union all
select
get_json_object(line,'$.connectionid'),
        get_json_object(line,'$.city'),
        get_json_object(line,'$.platform'),
        get_json_object(line,'$.version'),
        get_json_object(line,'$.clientype'),
        get_json_object(line,'$.network'),
        get_json_object(line, '$.cpu')      cpu,
        get_json_object(line, '$.gpu')      gpu,
        get_json_object(line,'$.accountid'),
        get_json_object(line,'$.resolution'),
        get_json_object(line,'$.vendor'),
        get_json_object(line,'$.clienttime'),
        null,
        null,
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.ConnType'),
        null,
        null,
        null,
        null,
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.IsRtc'),
        null,
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.HBytes'),
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.PBytes'),
        null,
        null,
        null,
        null,
        null,
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.DCS'),
        '-1',
        '-1',
        '-1',
        '-1',
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.RcvdFrMax'),
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.RcvdFrMin'),
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.RcvdFrMean'),
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.RcvdFrStd'),
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.DecFrMax'),
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.DecFrMin'),
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.DecFrMean'),
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.DecFrStd'),
        null,
        null,
        null,
        null,
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.RcvdRateMax'),
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.RcvdRateMin'),
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.RcvdRateMean'),
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.RcvdRateStd'),
        null,
        null,
        null,
        null,
        '-1',
        null,
        null,
        get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'), '$.tracks'),'$.[0]'),'$.VQp'),
	get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.VRtc'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.VStk'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.VCap'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.VEnc'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.VPac'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.VRec'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.VJit'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.VDec'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.VRtt'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.AcIdx'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.ActTime'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.HxFrTime'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.Stks'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.FrameCnts'),
        get_json_object(line,'$.logtime'),
        1,
        get_json_object(get_json_object(line, '$.turndata'),'$.rollback'),
        1
from ${APP}.ods_tuevent_log
where dt = '$do_date'
and get_json_object(line, '$.events') = 'link_info'
and get_json_object(line, '$.clientype') = 'client'
and get_json_object(line, '$.platform') = 'daas-web'
union all
select
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.city'),
    get_json_object(line,'$.clientplat'),
    get_json_object(line,'$.clientver'),
    get_json_object(line,'$.source'),
    get_json_object(line,'$.network'),
    get_json_object(line, '$.cpu')   ,
    get_json_object(line, '$.gpu')   ,
    get_json_object(line,'$.userid'),
    get_json_object(line,'$.resolution'),
    get_json_object(line,'$.vendor'),
    get_json_object(line,'$.clienttime'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.ConnTime'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.ConnState'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.ConnType'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.GatherState'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.SigState'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.IceState'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.StreamId'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.IsRTC'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.EBytes'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.HBytes'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.PBytes'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.RBytes'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.Dec') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.DecErr') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.DecErrCode') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.CodecName') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.DCS'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),  '$.Fst1') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.Fst2') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.Fst3') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.Fst4') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.LocalIpport') LocalIpport,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.LocalProto') LocalProto,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.LocalType') LocalType,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.RemotePort') RemotePort,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.RemoteProto') RemoteProto,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.RemoteType') RemoteType,
    '','','','',
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.DtlsDur') ,
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.RcvdFrMax'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.RcvdFrMin'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.RcvdFrMean'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.RcvdFrStd'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.DecFrMax'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.DecFrMin'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.DecFrMean'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.DecFrStd'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.RendFrMax'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.RendFrMin'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.RendFrMean'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.RendFrStd'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.RcvdRateMax'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.RcvdRateMin'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.RcvdRateMean'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.RcvdRateStd'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.DecStuckTime'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.DecStuckCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.LDecStuckTime'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.LDecStuckCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.FirstFrameTime'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.LastFrameTime'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.ReStoreCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.VQp'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.VRtc'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.VStk'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.VCap'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.VEnc'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.VPac'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.VRec'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.VJit'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.VDec'),
    get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'), '$.tracks'),'$.[0]'),'$.VRtt'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.AcIdx'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.ActTime'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.HxFrTime'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.Stks'),
get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.licode'),'$.[0]'),'$.[0]'),'$.tracks'),'$.[0]'),'$.FrameCnts'),
    get_json_object(get_json_object(line,'$.turndata'),'$.logtime'),
    1,
    get_json_object(get_json_object(line,'$.turndata'),'$.rollback'),
    1
from ${APP}.ods_other_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='3'
and get_json_object(line,'$.eventid') = 'link_info'
and get_json_object(line,'$.source') ='0';"

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
    get_json_object(get_json_object(line,'$.turndata'),'$.sdk_version'),
get_json_object(get_json_object(line,'$.turndata'),'$.duration'),
get_json_object(get_json_object(line,'$.turndata'),'$.connectionId'),
get_json_object(get_json_object(line,'$.turndata'),'$.fullConnectionId'),
get_json_object(get_json_object(line,'$.turndata'),'$.streamInfo'),
get_json_object(get_json_object(line,'$.turndata'),'$.hasRemoteDescription'),
get_json_object(get_json_object(line,'$.turndata'),'$.hasLocalDescription'),
get_json_object(get_json_object(line,'$.turndata'),'$.remoteIpAddress'),
get_json_object(get_json_object(line,'$.turndata'),'$.stateChange'),
get_json_object(get_json_object(line,'$.turndata'),'$.candidate_local_type'),
get_json_object(get_json_object(line,'$.turndata'),'$.candidate_local_protocol'),
get_json_object(get_json_object(line,'$.turndata'),'$.candidate_local_networktype'),
get_json_object(get_json_object(line,'$.turndata'),'$.candidate_local_address'),
get_json_object(get_json_object(line,'$.turndata'),'$.candidate_local_ipaddr'),
get_json_object(get_json_object(line,'$.turndata'),'$.candidate_local_port'),
get_json_object(get_json_object(line,'$.turndata'),'$.candidate_remote_type'),
get_json_object(get_json_object(line,'$.turndata'),'$.candidate_remote_protocol'),
get_json_object(get_json_object(line,'$.turndata'),'$.candidate_remote_networktype'),
get_json_object(get_json_object(line,'$.turndata'),'$.candidate_remote_address'),
get_json_object(get_json_object(line,'$.turndata'),'$.candidate_remote_ipaddr'),
get_json_object(get_json_object(line,'$.turndata'),'$.candidate_remote_port'),
get_json_object(get_json_object(line,'$.turndata'),'$.bytesReceived'),
get_json_object(get_json_object(line,'$.turndata'),'$.bytesSent'),
get_json_object(get_json_object(line,'$.turndata'),'$.packetsReceived'),
get_json_object(get_json_object(line,'$.turndata'),'$.packetsSent'),
get_json_object(get_json_object(line,'$.turndata'),'$.remoteOpenTcpPort'),
get_json_object(get_json_object(line,'$.turndata'),'$.track_audio'),
get_json_object(get_json_object(line,'$.turndata'),'$.track_video'),
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
    get_json_object(line,'$.logtime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.erizoType'),
    get_json_object(get_json_object(line,'$.turndata'),'$.signalingVersion'),
    get_json_object(get_json_object(line,'$.turndata'),'$.audioSendDuration'),
    get_json_object(get_json_object(line,'$.turndata'),'$.audioReceiveDuration'),
    get_json_object(get_json_object(line,'$.turndata'),'$.dcMaxSendErrorCount'),
    get_json_object(get_json_object(line,'$.turndata'),'$.dcCount'),
    get_json_object(get_json_object(line,'$.turndata'),'$.dcNoSendReadyCount'),
    get_json_object(get_json_object(line,'$.turndata'),'$.dcNoSendSucessCount'),
    get_json_object(get_json_object(line,'$.turndata'),'$.dcSendSucessSize'),
    get_json_object(get_json_object(line,'$.turndata'),'$.dcRecvSize'),
    get_json_object(get_json_object(line,'$.turndata'),'$.company_id')
from ${APP}.ods_tuevent_log
where dt='$do_date'
and get_json_object(line,'$.events')='licodesdk'
and get_json_object(line,'$.clientype') in ('client','host')
and get_json_object(get_json_object(line,'$.turndata'),'$.report_type')='ice';"

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
    get_json_object(line,'$.logtime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.erizoType'),
    get_json_object(get_json_object(line,'$.turndata'),'$.signalingVersion')
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
set hive.merge.smallfiles.avgsize=160000000;
set hive.merge.size.per.task=256000000;
insert overwrite table ${APP}.dwd_client_connect partition (dt='$do_date')
select
    if(get_json_object(line,'$.suid') is null,get_json_object(line,'$.tdid'), get_json_object(line,'$.suid')),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.sport'),
    get_json_object(line,'$.enterprise'),
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
    get_json_object(line,'$.badclient'),
    get_json_object(line,'$.client'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.pcname'),
    get_json_object(line,'$.systemtime'),
    get_json_object(line,'$.maclist'),
    get_json_object(line,'$.newmacid'),
    get_json_object(line,'$.logtime'),
    get_json_object(line,'$.sea')
from ${APP}.ods_client_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'client_connect';"

dwd_client_closed="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_client_closed partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.client'),
    nvl(get_json_object(line,'$.vsersion'),get_json_object(line,'$.version')),
    get_json_object(line,'$.ontime'),
    get_json_object(line,'$.line'),
    get_json_object(line,'$.province'),
    get_json_object(line,'$.result'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.oncomet'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_client_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'client_closed';"

dwd_web_config_center="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.merge.smallfiles.avgsize=160000000;
set hive.merge.size.per.task=256000000;
insert overwrite table ${APP}.dwd_web_config_center partition (dt='$do_date')
select
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.clientype'),
    get_json_object(line,'$.module'),
    get_json_object(line,'$.action'),
    get_json_object(line,'$.log'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_client_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='config_center';"


dwd_tmp_duid="
insert overwrite table ${APP}.dwd_tmp_duid partition (dt='$do_date')
select
    suid,'1',
	case when cast(${APP}.get_macid_hash(suid) as double) % 100 between 0 and 24 then '1'
             when cast(${APP}.get_macid_hash(suid) as double) % 100 between 25 and 49 then '2'
             when cast(${APP}.get_macid_hash(suid) as double) % 100 between 50 and 74 then '3'
             when cast(${APP}.get_macid_hash(suid) as double) % 100 between 75 and 99 then '4'
    end type
from ${APP}.dwd_new_connect
where dt='$do_date'
and sclient='PC'
and sversion>='4.5.0.0'
group by suid
union all
select
    duid,'2',
	case when cast(${APP}.get_macid_hash(duid) as double) % 100 between 0 and 24 then '1'
             when cast(${APP}.get_macid_hash(duid) as double) % 100 between 25 and 49 then '2'
             when cast(${APP}.get_macid_hash(duid) as double) % 100 between 50 and 74 then '3'
             when cast(${APP}.get_macid_hash(duid) as double) % 100 between 75 and 99 then '4'
    end type
from ${APP}.dwd_new_connect
where dt='$do_date'
and dclient='PC'
and dversion>='4.5.0.0'
group by duid;"

dwd_backend_banversion="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_banversion partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.srcplatform'),
    get_json_object(line,'$.dstplatform'),
    get_json_object(line,'$.srcversion'),
    get_json_object(line,'$.dstversion'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.dip'),
    get_json_object(line,'$.errcode'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='banversion';"

dwd_licode_connect_rtc="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_licode_connect_rtc partition (dt='$do_date')
select
    suid,duid,
    nvl(t1.connid,t2.connid),
    case
        when t1.connid is not null and t2.connid is not null then '1'
        when t1.connid is null and t2.connid is not null then '2'
        when t1.connid is not null and t2.connid is null then '3'
    end
from
(
    select
        get_json_object(line,'$.id') suid,
        get_json_object(get_json_object(line,'$.turndata'),'\$[0].connect_RTC') connid
    from ${APP}.ods_tuevent_log
    where dt='$do_date'
    and get_json_object(line,'$.events')='section'
    and get_json_object(line,'$.clientype')='client'
    and get_json_object(get_json_object(line,'$.turndata'),'\$[0].connect_RTC') is not null
    group by get_json_object(line,'$.id'),get_json_object(get_json_object(line,'$.turndata'),'\$[0].connect_RTC')
)t1
full outer join
(
    select
        get_json_object(line,'$.id') duid,
        get_json_object(get_json_object(line,'$.turndata'),'\$[0].connect_RTC') connid
    from ${APP}.ods_tuevent_log
    where dt='$do_date'
    and get_json_object(line,'$.events')='section'
    and get_json_object(line,'$.clientype')='host'
    and get_json_object(get_json_object(line,'$.turndata'),'\$[0].connect_RTC') is not null
    group by get_json_object(line,'$.id'),get_json_object(get_json_object(line,'$.turndata'),'\$[0].connect_RTC')
)t2
on t1.connid=t2.connid;"

dwd_client_close_fake="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_client_close_fake partition (dt='$do_date')
select 
    get_json_object(line,'$.fdkey'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.source'),
    get_json_object(line,'$.runtime'),
    get_json_object(line,'$.result'),
    get_json_object(line,'$.sport'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='client_close_fake';"

dwd_android_muses="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_android_muses partition (dt='$do_date')
select
    get_json_object(line,'$.id'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.events'),
    get_json_object(line,'$.platform'),
    get_json_object(line,'$.version'),
    get_json_object(line,'$.vendor'),
    get_json_object(line,'$.bios'),
    get_json_object(get_json_object(line,'$.turndata'),'$.comeRpFrameRate'),
    get_json_object(get_json_object(line,'$.turndata'),'$.comeTFrameRate'),
    get_json_object(get_json_object(line,'$.turndata'),'$.controlId'),
    get_json_object(get_json_object(line,'$.turndata'),'$.curMode'),
    get_json_object(get_json_object(line,'$.turndata'),'$.dataFlowNullptrCnt'),
    get_json_object(get_json_object(line,'$.turndata'),'$.drawAllTime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.frameJumpCnt'),
    get_json_object(get_json_object(line,'$.turndata'),'$.frameNullptrCnt'),
    get_json_object(get_json_object(line,'$.turndata'),'$.frameToDrawTime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.inputHeight'),
    get_json_object(get_json_object(line,'$.turndata'),'$.inputWidth'),
    get_json_object(get_json_object(line,'$.turndata'),'$.longStopFrequency'),
    get_json_object(get_json_object(line,'$.turndata'),'$.outputHeight'),
    get_json_object(get_json_object(line,'$.turndata'),'$.outputWidth'),
    get_json_object(get_json_object(line,'$.turndata'),'$.renderCycleTime'),
    get_json_object(get_json_object(line,'$.turndata'),'$.renderFrameCnt'),
    get_json_object(get_json_object(line,'$.turndata'),'$.shortStopFrequency'),
    get_json_object(get_json_object(line,'$.turndata'),'$.videoSourceId'),
    get_json_object(get_json_object(line,'$.turndata'),'$.yuvDrawTime'),
    get_json_object(line, '$.logtime')
from ${APP}.ods_tuevent_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='tuevent'
AND get_json_object(line,'$.events') ='muses_stat'
AND get_json_object(line,'$.action')='cycle_report';"

dwd_backend_licode_publish_ready="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_publish_ready partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.isrelay'),
    get_json_object(line,'$.isSFU'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='publish_ready';"

dwd_backend_licode_publish_start="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_publish_start partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.isrelay'),
    get_json_object(line,'$.isSFU'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='publish_start';"

dwd_backend_licode_subscribe_ready="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_subscribe_ready partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.isrelay'),
    get_json_object(line,'$.isSFU'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='subscribe_ready';"

dwd_backend_licode_subscribe_sfu="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_subscribe_sfu partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.isrelay'),
    get_json_object(line,'$.isSFU'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='subscribe_sfu';"

dwd_rtc_android_su="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_rtc_android_su partition (dt = '$do_date')
select
    *,
    max(r2)  over (partition by connid )
from
(
    select
        *,
        row_number() over (partition by connid order by clienttime) r2
    from
    (
        select
            get_json_object(line, '$.connectionid') connid,
            get_json_object(line, '$.city')        ,
            get_json_object(line, '$.platform')    ,
            get_json_object(line, '$.version')     ,
            get_json_object(line, '$.network')     ,
            get_json_object(line, '$.id')          ,
            get_json_object(line, '$.accountid')   ,
            get_json_object(line, '$.resolution')  ,
            get_json_object(line, '$.vendor')      ,
            get_json_object(line, '$.clienttime')  clienttime,
            get_json_object(line, '$.logtime')     ,
            get_json_object(get_json_object(line, '$.turndata'), '$.ConnTime'),
            get_json_object(get_json_object(line, '$.turndata'), '$.ConnState'),
            get_json_object(get_json_object(line, '$.turndata'), '$.ConnType'),
            get_json_object(get_json_object(line, '$.turndata'), '$.GatherState'),
            get_json_object(get_json_object(line, '$.turndata'), '$.SigState'),
            get_json_object(get_json_object(line, '$.turndata'), '$.IceState'),
            get_json_object(get_json_object(line, '$.turndata'), '$.StreamId'),
            get_json_object(get_json_object(line, '$.turndata'), '$.IsRTC'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.HBytes'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.PBytes'),
            get_json_object(get_json_object(line, '$.turndata'), '$.Fst1'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.Fst2'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.Fst3'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.Fst4'),
            get_json_object(get_json_object(line, '$.turndata'), '$.LocalIpport'),
            get_json_object(get_json_object(line, '$.turndata'), '$.LocalProto'),
            get_json_object(get_json_object(line, '$.turndata'), '$.LocalType'),
            get_json_object(get_json_object(line, '$.turndata'), '$.RemotePort'),
            get_json_object(get_json_object(line, '$.turndata'), '$.RemoteProto'),
            get_json_object(get_json_object(line, '$.turndata'), '$.RemoteType'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.Dec'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.RcvdFrMax'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.RcvdFrMin'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.RcvdFrMean'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.RcvdFrStd'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.DecFrMax'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.DecFrMin'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.DecFrMean'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.DecFrStd'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.RendFrMax'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.RendFrMin'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.RendFrMean'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.RendFrStd'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.RcvdRateMax'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.RcvdRateMin'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.RcvdRateMean'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.RcvdRateStd'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.DecStuckTime'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.DecStuckCnt'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.LDecStuckCnt'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.LDecStuckCnt'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.FirstFrameTime'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.LastFrameTime')
        from ${APP}.ods_tuevent_log
        where dt ='$do_date'
        and get_json_object(line,'$.events')='rtc_link_tech'
        and get_json_object(line,'$.action')='client'
        and get_json_object(line,'$.platform') in ('android','iOS')
        and get_json_object(get_json_object(line, '$.turndata'),'$.tracks') is not null
        and get_json_object(get_json_object(line, '$.turndata'),'$.tracks') <> 'null'
    )t1
)t2;"

dwd_rtc_android_du="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_rtc_android_du partition (dt = '$do_date')
select
    *,
    max(r2)  over (partition by connid )
from
(
    select
        *,
        row_number() over (partition by connid order by clienttime) r2
    from
    (
        select
            get_json_object(line, '$.connectionid') connid,
            get_json_object(line, '$.city')        ,
            get_json_object(line, '$.platform')    ,
            get_json_object(line, '$.version')     ,
            get_json_object(line, '$.network')     ,
            get_json_object(line, '$.id')          ,
            get_json_object(line, '$.accountid')   ,
            get_json_object(line, '$.resolution')  ,
            get_json_object(line, '$.vendor')      ,
            get_json_object(line, '$.clienttime')  clienttime,
            get_json_object(line, '$.logtime')     ,
            get_json_object(get_json_object(line, '$.turndata'), '$.ConnTime'),
            get_json_object(get_json_object(line, '$.turndata'), '$.ConnState'),
            get_json_object(get_json_object(line, '$.turndata'), '$.ConnType'),
            get_json_object(get_json_object(line, '$.turndata'), '$.GatherState'),
            get_json_object(get_json_object(line, '$.turndata'), '$.SigState'),
            get_json_object(get_json_object(line, '$.turndata'), '$.IceState'),
            get_json_object(get_json_object(line, '$.turndata'), '$.StreamId'),
            get_json_object(get_json_object(line, '$.turndata'), '$.IsRTC'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.EBytes '),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.FBytes '),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.PBytes'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.HBytes'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.RBytes'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.Enc'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.CodecName'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.EncErr'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.EncErrCode'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.Width'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.Height'),
            get_json_object(get_json_object(line, '$.turndata'), '$.SSMs'),
            get_json_object(get_json_object(line, '$.turndata'), '$.SSBwe'),
            get_json_object(get_json_object(line, '$.turndata'), '$.LocalIpport'),
            get_json_object(get_json_object(line, '$.turndata'), '$.LocalProto'),
            get_json_object(get_json_object(line, '$.turndata'), '$.LocalType'),
            get_json_object(get_json_object(line, '$.turndata'), '$.RemotePort'),
            get_json_object(get_json_object(line, '$.turndata'), '$.RemoteProto'),
            get_json_object(get_json_object(line, '$.turndata'), '$.RemoteType'),
            get_json_object(get_json_object(line, '$.turndata'), '$.CpuMax'),
            get_json_object(get_json_object(line, '$.turndata'), '$.CpuMin'),
            get_json_object(get_json_object(line, '$.turndata'), '$.CpuMean'),
            get_json_object(get_json_object(line, '$.turndata'), '$.CpuStd'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.CapFrMax'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.CapFrMin'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.CapFrMean'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.CapFrStd'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.QpMax'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.QpMin'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.QpMean'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.QpStd'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.SendFrMax'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.SendFrMin'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.SendFrMean'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.SendFrStd'),
            get_json_object(get_json_object(line, '$.turndata'),'$.TarRateMax'),
            get_json_object(get_json_object(line, '$.turndata'),'$.TarRateMin'),
            get_json_object(get_json_object(line, '$.turndata'),'$.TarRateMean'),
            get_json_object(get_json_object(line, '$.turndata'),'$.TarRateStd'),
            get_json_object(get_json_object(line, '$.turndata'),'$.ActRateMax'),
            get_json_object(get_json_object(line, '$.turndata'),'$.ActRateMin'),
            get_json_object(get_json_object(line, '$.turndata'),'$.ActRateMean'),
            get_json_object(get_json_object(line, '$.turndata'),'$.ActRateStd'),
            get_json_object(get_json_object(line, '$.turndata'),'$.RttMax'),
            get_json_object(get_json_object(line, '$.turndata'),'$.RttMin'),
            get_json_object(get_json_object(line, '$.turndata'),'$.RttMean'),
            get_json_object(get_json_object(line, '$.turndata'),'$.RttStd'),
            get_json_object(get_json_object(line, '$.turndata'),'$.LossMax'),
            get_json_object(get_json_object(line, '$.turndata'),'$.LossMin'),
            get_json_object(get_json_object(line, '$.turndata'),'$.LossMean'),
            get_json_object(get_json_object(line, '$.turndata'),'$.LossStd'),
            get_json_object(get_json_object(line, '$.turndata'),'$.RlLossMax'),
            get_json_object(get_json_object(line, '$.turndata'),'$.RlLossMin'),
            get_json_object(get_json_object(line, '$.turndata'),'$.RlLossMean'),
            get_json_object(get_json_object(line, '$.turndata'),'$.RlLossStd'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.SendRateMax'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.SendRateMin'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.SendRateMean'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.SendRateStd'),
            get_json_object(get_json_object(get_json_object(get_json_object(line, '$.turndata'),'$.tracks'), '$.[0]'),'$.VQT')
        from ${APP}.ods_tuevent_log
        where dt ='$do_date'
        and get_json_object(line,'$.events')='rtc_link_tech'
        and get_json_object(line,'$.action')='host'
        and get_json_object(line,'$.platform') in ('android','iOS')
        and get_json_object(get_json_object(line, '$.turndata'),'$.tracks') is not null
        and get_json_object(get_json_object(line, '$.turndata'),'$.tracks') <> 'null'
    )t1
)t2;"

dwd_tv_function_config="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_tv_function_config partition (dt='$do_date')
select id,order_id,user_id,mac_id,spu_id,status,type,value,level,start_time,end_time,create_time,update_time from ${APP}.ods_tv_function_config where dt='$do_date';"

dwd_plugin_check="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_plugin_check partition (dt='$do_date')
select
    get_json_object(line,'$.sid'),
    get_json_object(line,'$.did'),
    get_json_object(line,'$.userid'),
    get_json_object(line,'$.type'),
    get_json_object(line,'$.result_code'),
    get_json_object(line,'$.state'),
    get_json_object(line,'$.s_version'),
    get_json_object(line,'$.d_version'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt='$do_date'
and get_json_object(line,'$.logtype') = 'plugin_check';"

tmp_find_datas_mac="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.vectorized.execution.enabled = false;
set hive.merge.size.per.task=256000000;
insert overwrite table ${APP}.tmp_find_datas_mac partition (dt='$do_date')
select
    nvl(id,suid),
    ${APP}.array_distinct(${APP}.array_concat(if(t1.mac_list is null,\`array\`(),t1.mac_list),if(t2.mac_list is null,\`array\`(),t2.mac_list))),
    ${APP}.array_distinct(${APP}.array_concat(if(t1.pc_name is null,\`array\`(),t1.pc_name),if(t2.pc_name is null,\`array\`(),t2.pc_name)))
from
(
    select
        id, mac_list, pc_name
    from ${APP}.tmp_find_datas_mac
    where dt=date_sub('$do_date',1)
)t1
full outer join
(
    select
        suid,collect_set(mac) mac_list,collect_set(pc_name) pc_name
    from
     (
        select
            suid,split(if(mac_list is not null ,mac_list,''),';') mac_list,if(pc_name is not null ,pc_name,'') pc_name
        from ${APP}.dwd_client_connect
        where dt='$do_date'
        and suid is not null
     )t1
     lateral view explode(mac_list) tmp2 as mac
     group by suid
)t2
on t1.id=t2.suid;"

dwd_backend_licode_web_connection="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_web_connection partition (dt='$do_date')
select
    get_json_object(line,'$.duration'),
    concat_ws('_',split(get_json_object(line,'$.connectionId'),'_')[0],split(get_json_object(line,'$.connectionId'),'_')[1],split(get_json_object(line,'$.connectionId'),'_')[2]),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='WebConnection';"

dwd_anti_fraud_detail="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_anti_fraud_detail partition (dt='$do_date')
select
    suid,a_c,c1,rate
from
(
    select
        suid,sum(if(dclient='ANDROID',1,0)) a_c,count(1) c1,cast(sum(if(dclient='ANDROID',1,0)) / count(1) as decimal(16,4)) rate
    from
    (
        select
            suid,duid,dclient
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        group by suid,duid,dclient
    )t1
    group by suid
)t2
where a_c>=3 and rate >=0.8738;"

dwd_newfile_connect="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_newfile_connect partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.logtime'),
    if(get_json_object(line,'$.connid') is not null,get_json_object(line,'$.connid'),get_json_object(line,'$.connectionid')),
    get_json_object(line,'$.dip'),
    get_json_object(line,'$.disp'),
    get_json_object(line,'$.dstplatform'),
    get_json_object(line,'$.dstport'),
    get_json_object(line,'$.dstuser'),
    get_json_object(line,'$.dstvip'),
    get_json_object(line,'$.dstversion'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.id'),
    get_json_object(line,'$.ingroup'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.onServer'),
    get_json_object(line,'$.relayserver'),
    get_json_object(line,'$.relayservername'),
    get_json_object(line,'$.runtime'),
    get_json_object(line,'$.srcuser'),
    get_json_object(line,'$.srcvip'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.sisp'),
    get_json_object(line,'$.source'),
    get_json_object(line,'$.srcplatform'),
    get_json_object(line,'$.srcport'),
    get_json_object(line,'$.srcversion'),
    get_json_object(line,'$.tcp'),
    get_json_object(line,'$.tcpserver')
from ${APP}.ods_other_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'newfileconnect';"

dwd_connect_limit="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_connect_limit partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.flag'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.dip'),
    get_json_object(line,'$.srcplatform'),
    get_json_object(line,'$.dstplatform'),
    get_json_object(line,'$.srcversion'),
    get_json_object(line,'$.dstversion'),
    get_json_object(line,'$.source'),
    get_json_object(line,'$.srcvip'),
    get_json_object(line,'$.message'),
    get_json_object(line,'$.srcuser'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='connect_limit';"


dwd_connect_limit_sline="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_connect_limit_sline partition (dt='$do_date')
select
    get_json_object(line,'$.suid'),
    get_json_object(line,'$.duid'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.sip'),
    get_json_object(line,'$.dip'),
    get_json_object(line,'$.srcplatform'),
    get_json_object(line,'$.dstplatform'),
    get_json_object(line,'$.srcversion'),
    get_json_object(line,'$.dstversion'),
    get_json_object(line,'$.source'),
    get_json_object(line,'$.srcvip'),
    get_json_object(line,'$.srcuser'),
    get_json_object(line,'$.dstuser'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='Connect_Limit_Sline_Adhoc';"


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

dwd_daas_tv_user="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_daas_tv_user partition (dt='$do_date')
select id, phone, regdate, active_date, status, is_delete from ${APP}.ods_daas_tv_user where dt='$do_date';"

dwd_daas_tv_todesk_user="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_daas_tv_todesk_user partition (dt='$do_date')
select id, userid, todesk_userid, create_time, update_time from ${APP}.ods_daas_tv_todesk_user where dt='$do_date';"

dwd_vc_su="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_vc_su partition (dt='$do_date')
select
    get_json_object(line,'$.version'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.platform'),
    get_json_object(get_json_object(line,'$.turndata'),'$.label'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Order'),'$.Unorder_Cnt'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Packtizer'),'$.Pars_Fail_Cnt'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Packtizer'),'$.Pars_Succ_Cnt'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Packtizer'),'$.Retr_InVali_Cnt'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Packtizer'),'$.Retr_Vali_Cnt'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Resender'),'$.Loss_Cnt'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Resender'),'$.Resend_Cnt'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Resender'),'$.Send_Cnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.In'),'$.CPCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.In'),'$.DPNormCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.In'),'$.DPRetrCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.In'),'$.DPUnkCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.Out'),'$.CPCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.Out'),'$.DPNormCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.Out'),'$.DPRetrCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.Out'),'$.DPUnkCnt'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_tuevent_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='tuevent'
and get_json_object(line,'$.events')='licodesdk'
and get_json_object(line,'$.action')='vc_link_info'
and get_json_object(line,'$.clientype') = 'client';"

dwd_vc_du="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_vc_du partition (dt='$do_date')
select
    get_json_object(line,'$.version'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.platform'),
    get_json_object(get_json_object(line,'$.turndata'),'$.label'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Order'),'$.Unorder_Cnt'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Packtizer'),'$.Pars_Fail_Cnt'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Packtizer'),'$.Pars_Succ_Cnt'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Packtizer'),'$.Retr_InVali_Cnt'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Packtizer'),'$.Retr_Vali_Cnt'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Resender'),'$.Loss_Cnt'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Resender'),'$.Resend_Cnt'),
    get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.Resender'),'$.Send_Cnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.In'),'$.CPCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.In'),'$.DPNormCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.In'),'$.DPRetrCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.In'),'$.DPUnkCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.Out'),'$.CPCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.Out'),'$.DPNormCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.Out'),'$.DPRetrCnt'),
    get_json_object(get_json_object(get_json_object(get_json_object(line,'$.turndata'),'$.VCStats'),'$.Out'),'$.DPUnkCnt'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_tuevent_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='tuevent'
and get_json_object(line,'$.events')='licodesdk'
and get_json_object(line,'$.action')='vc_link_info'
and get_json_object(line,'$.clientype') = 'host';"

dwd_gass_consume="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_gass_consume partition (dt='$do_date')
select
    get_json_object(line,'$.agentid'),
    get_json_object(line,'$.agentplat'),
    get_json_object(line,'$.agentver'),
    get_json_object(line,'$.appid'),
    get_json_object(line,'$.appname'),
    get_json_object(line,'$.clientid'),
    get_json_object(line,'$.clientplat'),
    get_json_object(line,'$.clientver'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.costmoney'),
    get_json_object(line,'$.costtime'),
    get_json_object(line,'$.endtime'),
    get_json_object(line,'$.env'),
    get_json_object(line,'$.newpay'),
    get_json_object(line,'$.ordertype'),
    get_json_object(line,'$.processid'),
    get_json_object(line,'$.product'),
    get_json_object(line,'$.source'),
    get_json_object(line,'$.starttime'),
    get_json_object(line,'$.trigger'),
    get_json_object(line,'$.userid'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='0'
and get_json_object(line,'$.labtype')='cloudapp'
and get_json_object(line,'$.eventid')='consume';"

dwd_gass_gametab="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_gass_gametab partition (dt='$do_date')
select

    get_json_object(line,'$.clientplat'),
    get_json_object(line,'$.env'),
    get_json_object(line,'$.source'),
    get_json_object(line,'$.trigger'),
    get_json_object(line,'$.userid'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='0'
and get_json_object(line,'$.labtype')='cloudapp'
and get_json_object(line,'$.eventid')='gametab';"

dwd_gass_launchapp="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_gass_launchapp partition (dt='$do_date')
select
    get_json_object(line,'$.appid'),
    get_json_object(line,'$.appname'),
    get_json_object(line,'$.clientid'),
    get_json_object(line,'$.clientplat'),
    get_json_object(line,'$.clientver'),
    get_json_object(line,'$.env'),
    get_json_object(line,'$.newuser'),
    get_json_object(line,'$.userid'),
    get_json_object(line,'$.source'),
    get_json_object(line,'$.trigger'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='0'
and get_json_object(line,'$.labtype')='cloudapp'
and get_json_object(line,'$.eventid')='launchapp';"

dwd_newconnect_auth="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_newconnect_auth partition (dt='$do_date')
select
    get_json_object(line,'$.srcvip'),
    get_json_object(line,'$.did'),
    get_json_object(line,'$.dstvip'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.srccountry'),
    get_json_object(line,'$.dstcountry'),
    get_json_object(line,'$.Type'),
    get_json_object(line,'$.masterDid'),
    get_json_object(line,'$.srcPcName'),
    get_json_object(line,'$.srccity'),
    get_json_object(line,'$.Mode'),
    get_json_object(line,'$.dstcity'),
    get_json_object(line,'$.dstplatform'),
    get_json_object(line,'$.srcuser'),
    get_json_object(line,'$.from_svr'),
    get_json_object(line,'$.dstversion'),
    get_json_object(line,'$.token'),
    get_json_object(line,'$.dstuser'),
    get_json_object(line,'$.masterUid'),
    get_json_object(line,'$.srcversion'),
    get_json_object(line,'$.tdid'),
    get_json_object(line,'$.dstPcName'),
    get_json_object(line,'$.dstprovince'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.source'),
    get_json_object(line,'$.srcplatform'),
    get_json_object(line,'$.srcprovince'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='newconnect_auth';"

dwd_newconnect_auth_fail="
insert overwrite table ${APP}.dwd_newconnect_auth_fail partition (dt='$do_date')
select
    get_json_object(line,'$.source'),
    get_json_object(line,'$.tdid'),
    get_json_object(line,'$.Type'),
    get_json_object(line,'$.resultmsg'),
    get_json_object(line,'$.labtype'),
    get_json_object(line,'$.token'),
    get_json_object(line,'$.did'),
    get_json_object(line,'$.enterprise'),
    get_json_object(line,'$.from_svr'),
    get_json_object(line,'$.masterUid'),
    get_json_object(line,'$.reqDid'),
    get_json_object(line,'$.Mode'),
    get_json_object(line,'$.logtime')
from ${APP}.ods_other_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='newconnect_auth_fail';"

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
	dwd_android_licode_du )
	hive -e "$dwd_android_licode_du"
    ;;
    dwd_tmp_duid )
	hive -e "$dwd_tmp_duid"
    ;;
    dwd_backend_banversion )
	hive -e "$dwd_backend_banversion"
    ;;
    dwd_licode_connect_rtc )
	hive -e "$dwd_licode_connect_rtc"
    ;;
    dwd_licode_first_packet_receive )
	hive -e "$dwd_licode_first_packet_receive"
    ;;
    dwd_client_close_fake )
	hive -e "$dwd_client_close_fake"
    ;;
    dwd_android_muses )
	hive -e "$dwd_android_muses"
    ;;
    dwd_tv_function_config )
	hive -e "$dwd_tv_function_config"
    ;;
    dwd_plugin_check )
	hive -e "$dwd_plugin_check"
    ;;
    tmp_find_datas_mac )
	hive -e "$tmp_find_datas_mac"
    ;;
    dwd_backend_licode_web_connection )
	hive -e "$dwd_backend_licode_web_connection"
    ;;
    dwd_anti_fraud_detail )
	hive -e "$dwd_anti_fraud_detail"
    ;;
    dwd_newfile_connect )
	hive -e "$dwd_newfile_connect"
    ;;
    cc )
        hive -e "$dwd_rtc_android_su$dwd_rtc_android_du"
    ;;
    dwd_connect_limit )
	hive -e "$dwd_connect_limit"
    ;;
    dwd_connect_limit_sline )
	hive -e "$dwd_connect_limit_sline"
    ;;
    dwd_daas_tv_user )
	hive -e "$dwd_daas_tv_user"
    ;;
    dwd_daas_tv_todesk_user )
	hive -e "$dwd_daas_tv_todesk_user"
    ;;
    test )
	hive -e "$dwd_newconnect_auth$dwd_newconnect_auth_fail"
    ;;
    all_1 )
	hive -e "$dwd_sdk_su$dwd_sdk_du$dwd_rtc_android_su$dwd_rtc_android_du"
    ;;
    all_2 )
	hive -e "$dwd_backend_licode_publish_erizo$dwd_backend_licode_publish_initializing$dwd_backend_licode_subscribe_start$dwd_backend_licode_subscribe_p2p$dwd_new_connect_fail$dwd_client_pageview$dwd_web_config_center$dwd_licode_sdk$dwd_licode_sig$dwd_licode_ice$dwd_connection_closed$dwd_licode_session_success$dwd_media_statistic$dwd_web_request$dwd_client_section_su$dwd_client_section_du$dwd_licode_ice_connection$dwd_backend_licode_newconnect$dwd_backend_licode_reconnect$dwd_backend_licode_sdk_report$dwd_backend_licode_socket_disconnected$dwd_backend_licode_unpublish$dwd_backend_licode_unsubscribe$dwd_backend_banversion$dwd_licode_connect_rtc$dwd_licode_first_packet_receive$dwd_client_close_fake$dwd_android_muses"
    ;;
    all_3 )
	hive -e "$dwd_handshake_sd$dwd_authentication_success$dwd_session_success$dwd_new_connect$dim_su_info$dim_du_info$dwd_client_connect$dwd_client_closed$dwd_user_logout$dwd_handshake_success$dwd_controller_lost$dwd_presenter_lost$dwd_add_device$dwd_delete_device$dwd_edit_device$dwd_add_group$dwd_delete_group$dwd_edit_group"
    ;;
    all_4 )
	hive -e "$dwd_controller_TCP_packet_failure$dwd_presenter_TCP_packet_failure$dwd_new_user$dwd_user_login$dwd_wechat_login$dwd_phone_login$dwd_qrcode_login$dwd_auto_login$dwd_login_error$dwd_connection_failure$dwd_backend_licode_addsubscriber_ready$dwd_backend_licode_addPublisher_ready$dwd_backend_licode_all_ready$dwd_gass_gametab$dwd_newconnect_auth$dwd_newconnect_auth_fail"
    ;;
    all_5 )
	hive -e "$dwd_backend_licode_subscribe_end$dwd_backend_licode_publish_end$dwd_backend_licode_publish_ready$dwd_backend_licode_publish_start$dwd_backend_licode_subscribe_ready$dwd_backend_licode_subscribe_sfu$dwd_plugin_check$dwd_newfile_connect$dwd_connect_limit$dwd_connect_limit_sline$dwd_vc_su$dwd_vc_du$dwd_gass_consume$dwd_gass_launchapp"
    ;;
esac

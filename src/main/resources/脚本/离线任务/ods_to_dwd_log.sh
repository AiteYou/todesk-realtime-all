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
from
 (
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
  )t1
where get_json_object(line,'$.logtype') = 'handshake';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    and get_json_object(line,'$.logtype') = 'newconnect_fail'
    and get_json_object(line,'$.labtype') = 'logic'
    group by line,dt
)t1;"

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
from
 (
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
  )t1
where get_json_object(line,'$.logtype') = 'connection_stat';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    and get_json_object(line,'$.logtype') = 'authentication_success'
    group by line,dt
)t1;"

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
from
 (
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
  )t1
where get_json_object(line,'$.logtype') = 'client_connect'
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
from
 (
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
  )t1
where get_json_object(line,'$.logtype') = 'client_closed'
and get_json_object(line,'$.labtype') = 'logic';"

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
from
 (
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
  )t1
where get_json_object(line,'$.logtype') = 'user_logout'
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
from
 (
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
  )t1
where get_json_object(line,'$.logtype') = 'newconnect'
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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype') = 'handshake_success';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype') = 'controller_not_present';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype') = 'presenter_not_present';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype') = 'add_device'
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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype') = 'delete_device'
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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype') = 'edit_device'
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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype') = 'add_group'
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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype') = 'delete_group'
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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype') = 'edit_group'
and get_json_object(line,'$.labtype')= 'logic';"

dwd_client_tuevent="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.merge.sparkfiles=true;
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
from ${APP}.ods_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='tuevent';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='event';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='click';"

dwd_sdk_du="
set hive.support.quoted.identifiers=None;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_sdk_du  partition (dt='$do_date')
select
    \`(r1)?+.+\`,
    max(r2) over(partition by connectionid)
from (
         select *,
                row_number() over (partition by connectionid order by r1) r2
         from (
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
                          get_json_object(turndata_json,'$.ConnTime'),
                          get_json_object(turndata_json,'$.ConnState'),
                          get_json_object(turndata_json,'$.ConnType'),
                          get_json_object(turndata_json,'$.GatherState'),
                          get_json_object(turndata_json,'$.SigState'),
                          get_json_object(turndata_json,'$.IceState'),
                          get_json_object(turndata_json,'$.StreamId'),
                          get_json_object(turndata_json,'$.IsRTC'),
                          get_json_object(turndata_json,'$.EBytes'),
                          get_json_object(turndata_json,'$.HBytes'),
                          get_json_object(turndata_json,'$.PBytes'),
                          get_json_object(turndata_json,'$.RBytes'),
                          get_json_object(turndata_json,'$.Enc'),
                          get_json_object(turndata_json,'$.CodecName'),
                          get_json_object(turndata_json,'$.LocalIpport'),
                          get_json_object(turndata_json,'$.LocalProto'),
                          get_json_object(turndata_json,'$.LocalType'),
                          get_json_object(turndata_json,'$.RemotePort'),
                          get_json_object(turndata_json,'$.RemoteProto'),
                          get_json_object(turndata_json,'$.RemoteType'),
                          get_json_object(turndata_json,'$.CpuMax'),
                          get_json_object(turndata_json,'$.CpuMin'),
                          get_json_object(turndata_json,'$.CpuMean'),
                          get_json_object(turndata_json,'$.CpuStd'),
                          get_json_object(turndata_json,'$.CapFrMax'),
                          get_json_object(turndata_json,'$.CapFrMin'),
                          get_json_object(turndata_json,'$.CapFrMean'),
                          get_json_object(turndata_json,'$.CapFrStd'),
                          get_json_object(turndata_json,'$.QpMax'),
                          get_json_object(turndata_json,'$.QpMin'),
                          get_json_object(turndata_json,'$.QpMean'),
                          get_json_object(turndata_json,'$.QpStd'),
                          get_json_object(turndata_json,'$.SendFrMax'),
                          get_json_object(turndata_json,'$.SendFrMin'),
                          get_json_object(turndata_json,'$.SendFrMean'),
                          get_json_object(turndata_json,'$.SendFrStd'),
                          get_json_object(turndata_json,'$.TarRateMax'),
                          get_json_object(turndata_json,'$.TarRateMin'),
                          get_json_object(turndata_json,'$.TarRateMean'),
                          get_json_object(turndata_json,'$.TarRateStd'),
                          get_json_object(turndata_json,'$.ActRateMax'),
                          get_json_object(turndata_json,'$.ActRateMin'),
                          get_json_object(turndata_json,'$.ActRateMean'),
                          get_json_object(turndata_json,'$.ActRateStd'),
                          get_json_object(turndata_json,'$.RttMax'),
                          get_json_object(turndata_json,'$.RttMin'),
                          get_json_object(turndata_json,'$.RttMean'),
                          get_json_object(turndata_json,'$.RttStd'),
                          get_json_object(turndata_json,'$.LossMax'),
                          get_json_object(turndata_json,'$.LossMin'),
                          get_json_object(turndata_json,'$.LossMean'),
                          get_json_object(turndata_json,'$.LossStd'),
                          get_json_object(turndata_json,'$.RlLossMax'),
                          get_json_object(turndata_json,'$.RlLossMin'),
                          get_json_object(turndata_json,'$.RlLossMean'),
                          get_json_object(turndata_json,'$.RlLossStd'),
                          get_json_object(turndata_json,'$.SendRateMax'),
                          get_json_object(turndata_json,'$.SendRateMin'),
                          get_json_object(turndata_json,'$.SendRateMean'),
                          get_json_object(turndata_json,'$.SendRateStd'),
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
                                    from ${APP}.ods_log
                                    where dt = '$do_date'
                                      and get_json_object(line, '$.logtype') = 'tuevent'
                                      and get_json_object(line, '$.events') = 'link_info'
                                      and get_json_object(line, '$.clientype') = 'host'
                                      and get_json_object(line, '$.connectionid') <> ''
				      and get_json_object(line,'$.turndata') like '%[%'
                                      and size(split(regexp_replace(
                                                             regexp_replace(get_json_object(line, '$.turndata'), '\\\[|\\\]', ''),
                                                             '\\\}\\\,\\\{', '\\\}\\\;\\\{'), '\\\;')) > 0
                                    group by line
                                ) t6
                                    lateral view explode(split(
                                            regexp_replace(
                                                    regexp_replace(get_json_object(line, '$.turndata'), '\\\[|\\\]', ''),
                                                    '\\\}\\\,\\\{', '\\\}\\\;\\\{'),
                                            '\\\;')) ods_log_tmp as turndata_json
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
                          get_json_object(t5.turndata_json,'$.ConnTime'),
                          get_json_object(t5.turndata_json,'$.ConnState'),
                          get_json_object(t5.turndata_json,'$.ConnType'),
                          get_json_object(t5.turndata_json,'$.GatherState'),
                          get_json_object(t5.turndata_json,'$.SigState'),
                          get_json_object(t5.turndata_json,'$.IceState'),
                          get_json_object(t5.turndata_json,'$.StreamId'),
                          get_json_object(t5.turndata_json,'$.IsRTC'),
                          get_json_object(t5.turndata_json,'$.EBytes'),
                          get_json_object(t5.turndata_json,'$.HBytes'),
                          get_json_object(t5.turndata_json,'$.PBytes'),
                          get_json_object(t5.turndata_json,'$.RBytes'),
                          get_json_object(t5.turndata_json,'$.Enc'),
                          get_json_object(t5.turndata_json,'$.CodecName'),
                          get_json_object(t5.turndata_json,'$.LocalIpport'),
                          get_json_object(t5.turndata_json,'$.LocalProto'),
                          get_json_object(t5.turndata_json,'$.LocalType'),
                          get_json_object(t5.turndata_json,'$.RemotePort'),
                          get_json_object(t5.turndata_json,'$.RemoteProto'),
                          get_json_object(t5.turndata_json,'$.RemoteType'),
                          get_json_object(t5.turndata_json,'$.CpuMax'),
                          get_json_object(t5.turndata_json,'$.CpuMin'),
                          get_json_object(t5.turndata_json,'$.CpuMean'),
                          get_json_object(t5.turndata_json,'$.CpuStd'),
                          get_json_object(t5.turndata_json,'$.CapFrMax'),
                          get_json_object(t5.turndata_json,'$.CapFrMin'),
                          get_json_object(t5.turndata_json,'$.CapFrMean'),
                          get_json_object(t5.turndata_json,'$.CapFrStd'),
                          get_json_object(t5.turndata_json,'$.QpMax'),
                          get_json_object(t5.turndata_json,'$.QpMin'),
                          get_json_object(t5.turndata_json,'$.QpMean'),
                          get_json_object(t5.turndata_json,'$.QpStd'),
                          get_json_object(t5.turndata_json,'$.SendFrMax'),
                          get_json_object(t5.turndata_json,'$.SendFrMin'),
                          get_json_object(t5.turndata_json,'$.SendFrMean'),
                          get_json_object(t5.turndata_json,'$.SendFrStd'),
                          get_json_object(t5.turndata_json,'$.TarRateMax'),
                          get_json_object(t5.turndata_json,'$.TarRateMin'),
                          get_json_object(t5.turndata_json,'$.TarRateMean'),
                          get_json_object(t5.turndata_json,'$.TarRateStd'),
                          get_json_object(t5.turndata_json,'$.ActRateMax'),
                          get_json_object(t5.turndata_json,'$.ActRateMin'),
                          get_json_object(t5.turndata_json,'$.ActRateMean'),
                          get_json_object(t5.turndata_json,'$.ActRateStd'),
                          get_json_object(t5.turndata_json,'$.RttMax'),
                          get_json_object(t5.turndata_json,'$.RttMin'),
                          get_json_object(t5.turndata_json,'$.RttMean'),
                          get_json_object(t5.turndata_json,'$.RttStd'),
                          get_json_object(t5.turndata_json,'$.LossMax'),
                          get_json_object(t5.turndata_json,'$.LossMin'),
                          get_json_object(t5.turndata_json,'$.LossMean'),
                          get_json_object(t5.turndata_json,'$.LossStd'),
                          get_json_object(t5.turndata_json,'$.RlLossMax'),
                          get_json_object(t5.turndata_json,'$.RlLossMin'),
                          get_json_object(t5.turndata_json,'$.RlLossMean'),
                          get_json_object(t5.turndata_json,'$.RlLossStd'),
                          get_json_object(t5.turndata_json,'$.SendRateMax'),
                          get_json_object(t5.turndata_json,'$.SendRateMin'),
                          get_json_object(t5.turndata_json,'$.SendRateMean'),
                          get_json_object(t5.turndata_json,'$.SendRateStd'),
                         t5.logtime,
                         9999
                  from (
                           select get_json_object(line, '$.connectionid') conn_id
                           from (
                                    select line
                                    from ${APP}.ods_log
                                    where dt = '$do_date'
                                      and get_json_object(line, '$.logtype') = 'tuevent'
                                      and get_json_object(line, '$.events') = 'link_info'
                                      and get_json_object(line, '$.clientype') = 'host'
                                      and get_json_object(line, '$.connectionid') <> ''
				      and get_json_object(line,'$.turndata') like '%[%'
                                      and size(split(regexp_replace(
                                                             regexp_replace(get_json_object(line, '$.turndata'), '\\\[|\\\]', ''),
                                                             '\\\}\\\,\\\{', '\\\}\\\;\\\{'), '\\\;')) > 0 
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
                                   from ${APP}.ods_log
                                   where dt = '$do_date'
                                     and get_json_object(line, '$.logtype') = 'tuevent'
                                     and get_json_object(line, '$.events') = 'link_info'
                                     and get_json_object(line, '$.clientype') = 'client'
                                     and get_json_object(line, '$.connectionid') <> ''
                                     and get_json_object(get_json_object(line, '$.turndata'), '$.IsRTC') = '0'
                                   group by line
                               ) t4
                      ) t5
                      on t3.conn_id = t5.connectionid
                  where t5.connectionid is not null
              ) t8
     )t9;"

dwd_sdk_su="
set hive.support.quoted.identifiers=None;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_sdk_su  partition (dt='$do_date')
select
    \`(r1)?+.+\`,
    max(r2) over(partition by connectionid)
from (
         select *,
                row_number() over (partition by connectionid order by r1) r2
         from (
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
                          get_json_object(turndata_json, '$.LDecStuckCnt'),
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
                                    from ${APP}.ods_log
                                    where dt = '$do_date'
                                      and get_json_object(line, '$.logtype') = 'tuevent'
                                      and get_json_object(line, '$.events') = 'link_info'
                                      and get_json_object(line, '$.clientype') = 'client'
                                      and get_json_object(line, '$.connectionid') <> ''
				      and get_json_object(line,'$.turndata') like '[%'
                                      and size(split(regexp_replace(
                                                             regexp_replace(get_json_object(line, '$.turndata'), '\\\[|\\\]', ''),
                                                             '\\\}\\\,\\\{', '\\\}\\\;\\\{'), '\\\;')) >0 
                                    group by line
                                ) t6
                                    lateral view explode(split(
                                            regexp_replace(
                                                    regexp_replace(get_json_object(line, '$.turndata'), '\\\[|\\\]', ''),
                                                    '\\\}\\\,\\\{', '\\\}\\\;\\\{'),
                                            '\\;')) ods_log_tmp as turndata_json
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
                         get_json_object(t5.turndata_json, '$.LDecStuckCnt'),
                         get_json_object(t5.turndata_json, '$.LDecStuckCnt'),
                         get_json_object(t5.turndata_json, '$.FirstFrameTime'),
                         get_json_object(t5.turndata_json, '$.LastFrameTime'),
                         t5.logtime,
                         9999
                  from (
                           select get_json_object(line, '$.connectionid') conn_id
                           from (
                                    select line
                                    from ${APP}.ods_log
                                    where dt = '$do_date'
                                      and get_json_object(line, '$.logtype') = 'tuevent'
                                      and get_json_object(line, '$.events') = 'link_info'
                                      and get_json_object(line, '$.clientype') = 'client'
                                      and get_json_object(line, '$.connectionid') <> ''
				      and get_json_object(line,'$.turndata') like '[%'
                                      and size(split(regexp_replace(
                                                             regexp_replace(get_json_object(line, '$.turndata'), '\\\[|\\\]', ''),
                                                             '\\\}\\\,\\\{', '\\\}\\\;\\\{'), '\\\;')) > 0 
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
                                   from ${APP}.ods_log
                                   where dt = '$do_date'
                                     and get_json_object(line, '$.logtype') = 'tuevent'
                                     and get_json_object(line, '$.events') = 'link_info'
                                     and get_json_object(line, '$.clientype') = 'client'
                                     and get_json_object(line, '$.connectionid') <> ''
                                     and get_json_object(get_json_object(line, '$.turndata'), '$.IsRTC') = '0'
                                   group by line
                               ) t4
                      ) t5
                                     on t3.conn_id = t5.connectionid
                  where t5.connectionid is not null
              ) t8
     )t9;"


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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='tevent';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='controller_TCP_packet_failure';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='presenter_TCP_packet_failure';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='newuser';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='user_login';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='wechatLogin';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='phoneLogin';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='qrcodeLogin';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='autoLogin';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='errorlog';"

dwd_connection_failure="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_connection_failure partition (dt='$do_date')
select
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.logtype'),
    get_json_object(line,'$.logtime')
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype') in ('presenter_not_present','controller_not_present','all_not_present','controller_TCP_packet_failure','presenter_TCP_packet_failure');"



dwd_backend_licode_addsubscriber_ready="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_addsubscriber_ready partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.clientid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.logtime')
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='addSubscriber_ready';"

dwd_backend_licode_addPublisher_ready="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_addPublisher_ready partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.clientid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.logtime')
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='addPublisher_ready';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='publish_erizo';"

dwd_backend_licode_publish_initializing="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_publish_initializing partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.logtime')
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='publish_initializing';"

dwd_backend_licode_subscribe_start="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_subscribe_start partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.timestamp'),
    get_json_object(line,'$.logtime')
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='subscribe_start';"

dwd_backend_licode_subscribe_p2p="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_backend_licode_subscribe_p2p partition (dt='$do_date')
select
    get_json_object(line,'$.ip'),
    get_json_object(line,'$.connectionid'),
    get_json_object(line,'$.streamid'),
    get_json_object(line,'$.logtime')
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='subscribe_p2p';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='config_center';"

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
from ${APP}.ods_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='tuevent'
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
from ${APP}.ods_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='tuevent'
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
from ${APP}.ods_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='tuevent'
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
from ${APP}.ods_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='tuevent'
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
from ${APP}.ods_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='tuevent'
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
from ${APP}.ods_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='tuevent'
and get_json_object(line,'$.events')='rtc_link_tech'
and get_json_object(line,'$.action') in ('link_start','link_result');"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='connection_closed';"

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
from ${APP}.ods_log
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
from ${APP}.ods_log
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
from ${APP}.ods_log
where dt ='$do_date'
and get_json_object(line,'$.logtype')='request'
and get_json_object(line,'$.suid') is not null;"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='tuevent'
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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='tuevent'
and get_json_object(line,'$.events')='section'
and get_json_object(line,'$.clientype')='client';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='tuevent'
and get_json_object(line,'$.events')='section'
and get_json_object(line,'$.clientype')='host';"

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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype') ='pageview';"

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
from ${APP}.ods_log
where dt='$do_date'
and get_json_object(line,'$.logtype')='tuevent'
and get_json_object(line,'$.events')='licodesdk'
and get_json_object(line,'$.clientype') in ('client','host')
and get_json_object(get_json_object(line,'$.turndata'),'$.report_type')='iceConnectionState';"

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
    from
    (
        select
            line,
            dt,
            count(*)
        from ${APP}.ods_log
        where dt ='$do_date'
        group by line,dt
    )t1
    where get_json_object(line,'$.logtype')='licode_newconnection'
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
from
(
    select
        line,
        dt,
        count(*)
    from ${APP}.ods_log
    where dt ='$do_date'
    group by line,dt
)t1
where get_json_object(line,'$.logtype')='licode_reconnection';"

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
from ${APP}.ods_log
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
from ${APP}.ods_log
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
from ${APP}.ods_log
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
from ${APP}.ods_log
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
from ${APP}.ods_log
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
from ${APP}.ods_log
where dt ='$do_date'
and get_json_object(line,'$.logtype') = 'publish_end';"

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
    cc )
        hive -e "$dwd_backend_licode_addsubscriber_ready$dwd_backend_licode_addPublisher_ready$dwd_backend_licode_all_ready$dwd_backend_licode_publish_erizo$dwd_backend_licode_publish_initializing$dwd_backend_licode_subscribe_start$dwd_backend_licode_subscribe_p2p$dwd_backend_licode_unsubscribe$dwd_backend_licode_unpublish$dwd_backend_licode_sdk_report$dwd_backend_licode_subscribe_end$dwd_backend_licode_publish_end"
    ;;
    all )
        hive -e "$dwd_handshake_sd$dwd_authentication_success$dwd_session_success$dwd_new_connect$dwd_client_connect$dwd_client_closed$dwd_user_logout$dwd_handshake_success$dwd_controller_lost$dwd_presenter_lost$dwd_add_device$dwd_delete_device$dwd_edit_device$dwd_add_group$dwd_delete_group$dwd_edit_group$dwd_client_tuevent$dwd_client_event$dwd_client_click$dwd_client_tevent$dwd_controller_TCP_packet_failure$dwd_presenter_TCP_packet_failure$dwd_new_user$dwd_user_login$dwd_wechat_login$dwd_phone_login$dwd_qrcode_login$dwd_auto_login$dwd_login_error$dwd_connection_failure$dwd_sdk_du$dwd_sdk_su$dwd_backend_licode_addsubscriber_ready$dwd_backend_licode_addPublisher_ready$dwd_backend_licode_all_ready$dwd_backend_licode_publish_erizo$dwd_backend_licode_publish_initializing$dwd_backend_licode_subscribe_start$dwd_backend_licode_subscribe_p2p$dwd_web_config_center$dwd_licode_sdk$dwd_licode_sig$dwd_licode_ice$dwd_android_licode_su$dwd_android_state_machine$dwd_android_link_start_result$dwd_connection_closed$dwd_tv_try$dwd_ord$dwd_licode_session_success$dwd_media_statistic$dwd_web_request$dwd_link_info$dwd_client_section_su$dwd_client_section_du$dwd_new_connect_fail$dwd_client_pageview$dwd_licode_ice_connection$dwd_backend_licode_newconnect$dwd_backend_licode_reconnect$dwd_backend_licode_sdk_report$dwd_backend_licode_socket_disconnected$dwd_backend_licode_unpublish$dwd_backend_licode_unsubscribe"
    ;;
esac

#!/bin/bash

APP=todesk
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

dws_serv_total="
insert overwrite table ${APP}.dws_serv_total partition (dt='$do_date')
select
    serv,
    s_client,
    d_client,
    sum(if(handshake_success_connid is not null,1,0)) handshake_success_count,
    sum(if(authentication_success_connid is not null,1,0)) authentication_success_count,
    count(1) new_connect_count,
    cast(sum(if(handshake_success_connid is not null,1,0)) / count(1) * 100 as decimal(16,2)) handshake_success_rate,
    cast(sum(if(authentication_success_connid is not null,1,0)) / count(1) * 100 as decimal(16,2)) authentication_success_rate,
    cast(avg((\`out\`-\`in\`) / link_time) AS DECIMAL(16,2) ) bitrate
from
(
    select connid,
	   case
               when sclient='1' then 'PC'
               when sclient='2' then 'MAC'
               when sclient='3' then 'IOS'
               when sclient='4' then 'ANDROID'
               when sclient='5' then 'LINUX'
               when sclient='6' then 'ONTHER'
	       when sclient='9' then 'MOBILE_OHOS'
               when sclient='10' then 'PC_OHOS'
            else sclient end s_client,
           case
               when dclient='1' then 'PC'
               when dclient='2' then 'MAC'
               when dclient='3' then 'IOS'
               when dclient='4' then 'ANDROID'
               when dclient='5' then 'LINUX'
               when dclient='6' then 'ONTHER'
	       when sclient='9' then 'MOBILE_OHOS'
               when sclient='10' then 'PC_OHOS'
            else dclient end d_client,
           relay_server_name serv
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and labtype!='cloudapp'
)t1
left join
(
    select
           connid authentication_success_connid
    from ${APP}.dwd_authentication_success
    where dt='$do_date'
    group by connid
)t2
on t1.connid=t2.authentication_success_connid
left join
(
    select connid handshake_success_connid
    from ${APP}.dwd_handshake_success
    where dt='$do_date'
    group by connid
)t3
on t1.connid=t3.handshake_success_connid
left join
(
    select
           connid session_success_connid,
           \`out\`,
           \`in\`,
           link_time
    from ${APP}.dwd_session_success
    where dt='$do_date'
    and \`out\` <> 0
    group by connid,\`out\`,\`in\`,link_time
)t4
on t1.connid=t4.session_success_connid
group by t1.serv,s_client,d_client 
order by t1.serv,s_client desc,d_client desc;"

dws_presenter_lost_total="
insert overwrite table ${APP}.dws_presenter_lost_total partition (dt='$do_date')
select
    t1.relay_server_name,
    sum(if(t2.connid is not null,1,0)),
    count(1),
    cast(sum(if(t2.connid is not null,1,0)) / count(1)*100 as DECIMAL(16,2)) c1
from
(
    select
        connid,
        relay_server_name
    from ${APP}.dwd_new_connect
    where dt ='$do_date'
)t1
left join
(
    select
        connid
    from ${APP}.dwd_presenter_lost
    where dt='$do_date'
    group by connid
)t2
on t1.connid=t2.connid
group by t1.relay_server_name
order by c1 desc;"

dws_bus_user_action="
insert overwrite table ${APP}.dws_bus_user_action partition(dt='$do_date')
select
    account_id,
    bus,
    action,
    count(*) 
from ${APP}.dwd_client_event
where dt = '$do_date'
and account_id is not null
and account_id <> ''
and bus is not null
and bus <> ''
and action in('start_front','start_process')
group by account_id,bus,action
order by cast(account_id as BIGINT);"

dws_user_action="
insert overwrite table ${APP}.dws_user_action partition(dt='$do_date')
select
    account_id,
    action,
    count(*) 
from ${APP}.dwd_client_event
where dt = '$do_date'
and account_id is not null
and account_id <> ''
and action in('start_front','start_process')
group by account_id,action
order by cast(account_id as BIGINT);"

dws_mac_action="
insert overwrite table ${APP}.dws_mac_action partition(dt='$do_date')
select
    mac_id,
    platform,
    action,
    count(*) 
from ${APP}.dwd_client_event
where dt = '$do_date'
and mac_id is not null
and mac_id <> ''
and action in('start_front','start_process')
group by mac_id,platform,action
order by cast(mac_id as BIGINT);"

dws_su_action="
insert overwrite table ${APP}.dws_su_action partition(dt='$do_date')
select
    t1.suid,
    t2.platform,
    t2.action,
    t2.cnt
from
(
    select
        suid,
        count(*)
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    group by suid
)t1
left join
(
    select
        mac_id,
        platform,
        action,
        count(*) cnt 
    from ${APP}.dwd_client_event
    where dt = '$do_date'
    and mac_id is not null
    and mac_id <> ''
    and action in('start_front','start_process')
    group by mac_id,platform,action
)t2
on t1.suid=t2.mac_id;"

dws_du_action="
insert overwrite table ${APP}.dws_du_action partition(dt='$do_date')
select
    t1.duid,
    t2.platform,
    t2.action,
    t2.cnt
from
(
    select
        duid,
        count(*)
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    group by duid
)t1
left join
(
    select
        mac_id,
        platform,
        action,
        count(*) cnt 
    from ${APP}.dwd_client_event
    where dt = '$do_date'
    and mac_id is not null
    and mac_id <> ''
    and action in('start_front','stardt_process')
    group by mac_id,platform,action
)t2
on t1.duid=t2.mac_id;"


dws_hour_total="
insert overwrite table ${APP}.dws_hour_total partition (dt='$do_date')
select
    t5.hour,
    t5.authentication_success_count,
    t5.new_connect_count,
    t5.session_rate,
    t6.bitrate
from
(
    select
        hour,
        sum(if(success_connid is not null,1,0)) authentication_success_count,
        count(1) new_connect_count,
        cast(sum(if(success_connid is not null,1,0)) /count(t1.connid) * 100 as decimal(16,2)) session_rate
    from
    (
        select connid,
               date_format(from_utc_timestamp(ts,'Asia/Shanghai'),'HH') hour
        from ${APP}.dwd_new_connect
        where dt='$do_date'
    )t1
    left join
    (
        select
               connid success_connid
        from ${APP}.dwd_authentication_success
        where dt='$do_date'
        group by connid
    )t2
    on t1.connid=t2.success_connid
    group by t1.hour
)t5
left join
(
    select
        hour,
        cast(avg((\`out\`-\`in\`) / link_time) AS DECIMAL(16,2) ) bitrate
    from
    (
        select connid,
               date_format(from_utc_timestamp(ts,'Asia/Shanghai'),'HH') hour
        from ${APP}.dwd_new_connect
        where dt='$do_date'
    )t3
    left join
    (
        select
               connid success_connid,
               \`out\`,
               \`in\`,
               link_time
        from ${APP}.dwd_session_success
        where dt='$do_date'
        group by connid,\`out\`,\`in\`,link_time
    )t4
    on t3.connid=t4.success_connid
    group by t3.hour
)t6
on t5.hour=t6.hour
order by t5.hour;"

dws_serv_connection_failure="
insert overwrite table ${APP}.dws_serv_connection_failure partition (dt='$do_date')
select
    serv s1,
    failure_type f1,
    c1,
    sum(c1) over(partition by serv),
    cast(c1/sum(c1) over(partition by serv) * 100 as DECIMAL(16,2))
from
(
    select
        serv,
        failure_type,
        count(1) c1
    from
    (
        select connid,
               relay_server_name serv
        from ${APP}.dwd_new_connect
        where dt='$do_date'
    )t1
    right join
    (
        select
               connid,
               failure_type
        from ${APP}.dwd_connection_failure
        where dt='$do_date'
        group by connid,failure_type
    )t2
    on t1.connid=t2.connid
    group by t1.serv,t2.failure_type
)t3
where serv is not null
order by s1,f1;"

dws_su_action_daytotal="
insert overwrite table ${APP}.dws_su_action_daytotal partition(dt='$do_date')
select
    suid,
    sclient
from
 (
    select
        suid,
        case
            when sclient='0' then 'UNIVERSAL'
            when sclient='1' then 'PC'
            when sclient='2' then 'MAC'
            when sclient='3' then 'IOS'
            when sclient='4' then 'ANDROID'
            when sclient='5' then 'LINUX'
            when sclient='6' then 'ONTHER'
            else sclient
        end sclient
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and suid is not null
    and sclient is not null
 )t1
group by suid,sclient;"

dws_du_action_daytotal="
insert overwrite table ${APP}.dws_du_action_daytotal partition(dt='$do_date')
select
    duid,
    dclient
from
 (
    select
        duid,
        case
            when dclient='0' then 'UNIVERSAL'
            when dclient='1' then 'PC'
            when dclient='2' then 'MAC'
            when dclient='3' then 'IOS'
            when dclient='4' then 'ANDROID'
            when dclient='5' then 'LINUX'
            when dclient='6' then 'ONTHER'
            else dclient
        end dclient
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and duid is not null
    and dclient is not null
 )t1
group by duid,dclient;"

dws_client_daytotal="
insert overwrite table ${APP}.dws_client_daytotal partition(dt='$do_date')
select
    mac_id, client_type
from ${APP}.dwd_logic_online
where dt='$do_date'
and user_id = '0' 
group by mac_id,client_type;"


dws_province_customer_day_total="
insert overwrite table ${APP}.dws_province_customer_day_total partition (dt='$do_date')
select
    type,
    province_name,
    su_isp,
    s_client,
    d_client,
    iso_code,
    customer_type,
    handshake_success_count,
    authentication_success_count,
    new_connect_count,
    handshake_success_rate,
    authentication_success_rate,
    bitrate
from ${APP}.dim_base_province area
left join
(
    select
        'controller' type,
        sprovince,
        su_isp,
        s_client,
        d_client,
        customer_type,
        sum(if(handshake_success_connid is not null,1,0)) handshake_success_count,
        sum(if(authentication_success_connid is not null,1,0)) authentication_success_count,
        count(1) new_connect_count,
        cast(sum(if(handshake_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) handshake_success_rate,
        cast(sum(if(authentication_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) authentication_success_rate,
        cast(avg((\`out\`-\`in\`) / link_time) AS DECIMAL(16,2) ) bitrate
    from
    (
        select connid,
               sprovince,
               case
                   when enterprise='0' then 'customer'
                   when enterprise='1' then 'business'
               else 'other' end customer_type,
               case
                   when sisp='中国联通' then '中国联通'
                   when sisp='中国移动' then '中国移动'
                   when sisp='中国电信' then '中国电信'
		   when sline='鹏博士'  then '鹏博士'
               else 'other' end su_isp,
               case
                   when sclient='1' then 'PC'
                   when sclient='2' then 'MAC'
                   when sclient='3' then 'IOS'
                   when sclient='4' then 'ANDROID'
                   when sclient='5' then 'LINUX'
                   when sclient='6' then 'ONTHER'
		   when sclient='9' then 'MOBILE_OHOS'
               when sclient='10' then 'PC_OHOS'
                else sclient end s_client,
               case
                   when dclient='1' then 'PC'
                   when dclient='2' then 'MAC'
                   when dclient='3' then 'IOS'
                   when dclient='4' then 'ANDROID'
                   when dclient='5' then 'LINUX'
                   when dclient='6' then 'ONTHER'
		  when sclient='9' then 'MOBILE_OHOS'
               when sclient='10' then 'PC_OHOS'
                else dclient end d_client
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        and sprovince in ('广东省','江苏省','上海市','浙江省','福建省','山东省','河南省','四川省','北京市','河北省','辽宁省','吉林省','湖南省','安徽省','湖北省','江西省','广西壮族自治区','黑龙江省','陕西省','山西省','重庆市','贵州省','天津市','云南省','内蒙古自治区','新疆维吾尔自治区','甘肃省','中国香港','海南省','宁夏回族自治区','中国台湾','青海省','西藏自治区','澳门特别行政区')
        and enterprise in ('0','1')
    )t1
    left join
    (
        select
               connid authentication_success_connid
        from ${APP}.dwd_authentication_success
        where dt='$do_date'
        group by connid
    )t2
    on t1.connid=t2.authentication_success_connid
    left join
    (
        select
            connid,
            \`out\`,
            \`in\`,
            link_time
        from ${APP}.dwd_session_success
        where dt='$do_date'
        and \`out\` <> 0
        group by connid,\`out\`,\`in\`,link_time
    )t3
    on t1.connid=t3.connid
    left join
    (
        select
            connid handshake_success_connid
        from ${APP}.dwd_handshake_success
        where dt='$do_date'
        group by connid
    )t4
    on t1.connid=t4.handshake_success_connid
    group by sprovince,customer_type,su_isp,s_client,d_client
) su_session_rate
on area.province_name=su_session_rate.sprovince
union all
select
    type,
    province_name,
    du_isp,
    s_client,
    d_client,
    iso_code,
    customer_type,
    handshake_success_count,
    authentication_success_count,
    new_connect_count,
    handshake_success_rate,
    authentication_success_rate,
    bitrate
from ${APP}.dim_base_province area
left join
(
    select
        'presenter' type,
        dprovince,
        du_isp,
        s_client,
        d_client,
        customer_type,
        sum(if(handshake_success_connid is not null,1,0)) handshake_success_count,
        sum(if(authentication_success_connid is not null,1,0)) authentication_success_count,
        count(1) new_connect_count,
        cast(sum(if(handshake_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) handshake_success_rate,
        cast(sum(if(authentication_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) authentication_success_rate,
        cast(avg((\`out\`-\`in\`) / link_time) AS DECIMAL(16,2) ) bitrate
    from
    (
        select connid,
               dprovince,
               case
                   when enterprise='0' then 'customer'
                   when enterprise='1' then 'business'
               else 'other' end customer_type,
               case
                   when disp='中国联通' then '中国联通'
                   when disp='中国移动' then '中国移动'
                   when disp='中国电信' then '中国电信'
  		   when dline='鹏博士'  then '鹏博士'
               else 'other' end du_isp,
	       case
                   when sclient='1' then 'PC'
                   when sclient='2' then 'MAC'
                   when sclient='3' then 'IOS'
                   when sclient='4' then 'ANDROID'
                   when sclient='5' then 'LINUX'
                   when sclient='6' then 'ONTHER'
		   when sclient='9' then 'MOBILE_OHOS'
               when sclient='10' then 'PC_OHOS'
                else sclient end s_client,
               case
                   when dclient='1' then 'PC'
                   when dclient='2' then 'MAC'
                   when dclient='3' then 'IOS'
                   when dclient='4' then 'ANDROID'
                   when dclient='5' then 'LINUX'
                   when dclient='6' then 'ONTHER'
	           when sclient='9' then 'MOBILE_OHOS'
               when sclient='10' then 'PC_OHOS'
                else dclient end d_client
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        and dprovince in ('广东省','江苏省','上海市','浙江省','福建省','山东省','河南省','四川省','北京市','河北省','辽宁省','吉林省','湖南省','安徽省','湖北省','江西省','广西壮族自治区','黑龙江省','陕西省','山西省','重庆市','贵州省','天津市','云南省','内蒙古自治区','新疆维吾尔自治区','甘肃省','中国香港','海南省','宁夏回族自治区','中国台湾','青海省','西藏自治区','澳门特别行政区')
        and enterprise in ('0','1')
    )t5
    left join
    (
        select
               connid authentication_success_connid
        from ${APP}.dwd_authentication_success
        where dt='$do_date'
        group by connid
    )t6
    on t5.connid=t6.authentication_success_connid
    left join
    (
        select
            connid,
            \`out\`,
            \`in\`,
            link_time
        from ${APP}.dwd_session_success
        where dt='$do_date'
        and \`out\` <> 0
        group by connid,\`out\`,\`in\`,link_time
    )t7
    on t5.connid=t7.connid
    left join
    (
        select
            connid handshake_success_connid
        from ${APP}.dwd_handshake_success
        where dt='$do_date'
        group by connid
    )t8
    on t5.connid=t8.handshake_success_connid
    group by dprovince,customer_type,du_isp,s_client,d_client
) du_session_rate
on area.province_name=du_session_rate.dprovince
order by type,province_name,su_isp;"

dws_country_vip_total="
insert overwrite table ${APP}.dws_country_vip_total partition (dt='$do_date')
select
    scountry,
    svip,
    dcountry,
    dvip,
    sum(if(handshake_success_connid is not null,1,0)) handshake_success_count,
    sum(if(authentication_success_connid is not null,1,0)) authentication_success_count,
    count(1) new_connect_count,
    cast(sum(if(handshake_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) handshake_success_rate,
    cast(sum(if(authentication_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) authentication_success_rate,
    cast(avg((\`out\`-\`in\`) / link_time) AS DECIMAL(16,2) ) bitrate
from
(
    select
           connid,
           scountry,
            svip,
           dcountry,
           dvip
    from ${APP}.dwd_new_connect
    where dt='$do_date'
)t1
left join
(
    select
           connid authentication_success_connid
    from ${APP}.dwd_authentication_success
    where dt='$do_date'
    group by connid
)t2
on t1.connid=t2.authentication_success_connid
left join
(
    select
        connid,
        \`out\`,
        \`in\`,
        link_time
    from ${APP}.dwd_session_success
    where dt='$do_date'
    and \`out\` <> 0
    group by connid,\`out\`,\`in\`,link_time
)t3
on t1.connid=t3.connid
left join
(
    select
        connid handshake_success_connid
    from ${APP}.dwd_handshake_success
    where dt='$do_date'
    group by connid
)t4
on t1.connid=t4.handshake_success_connid
group by scountry,svip,dcountry,dvip
order by scountry,dcountry,svip,dvip;"

dws_ip_licode_day_total="
insert overwrite table ${APP}.dws_ip_licode_day_total partition (dt='$do_date')
select  
    ip,
    sum(if(t8.streamid is not null ,1,0)),
    count(1),
    cast(sum(if(t8.streamid is not null ,1,0))  / count(1) *100 as DECIMAL(16,2)) r1
from
(
    select  
        nvl(t5.ip,t6.ip) ip,
        nvl(t5.streamid,t6.streamid) streamid
    from
    (
        select  
            t1.*
        from
        (
            select
                ip,
                streamid
            from ${APP}.dwd_backend_licode_publish_erizo
            where dt='$do_date'
            group by ip, streamid
        )t1
        left join
        (
            select
                streamid
            from ${APP}.dwd_backend_licode_publish_initializing
            where dt='$do_date'
            group by streamid
        )t2
        on t1.streamid=t2.streamid
        where t1.streamid is not null and t2.streamid is null
    )t5
    full outer join
    (
        select 
            t3.*
        from
        (
            select
                ip,
                streamid
            from ${APP}.dwd_backend_licode_subscribe_start 
            where dt='$do_date'
            group by ip, streamid
        )t3
        left join
        (
            select
                streamid
            from ${APP}.dwd_backend_licode_subscribe_p2p 
            where dt='$do_date'
            group by streamid
        )t4
        on t3.streamid=t4.streamid
        where t3.streamid is not null and t4.streamid is null
    )t6
    on t5.streamid=t6.streamid
    where t5.streamid is null or t6.streamid is null
)t7
left join
(
    select
        streamid
    from ${APP}.dwd_backend_licode_all_ready
    where dt='$do_date'
)t8
on t7.streamid=t8.streamid
group by ip
order by r1;"

dws_client_version_licode_day_total="
insert overwrite table ${APP}.dws_client_version_licode_day_total partition (dt='$do_date')
select
    sclient,dclient,
    t11.sversion,t11.dversion,
    sum(if(t12.streamid is not null ,1,0)),
    count(1),
    cast(sum(if(t12.streamid is not null ,1,0))  / count(1) *100 as DECIMAL(16,2))
from
(
    select  
        t7.streamid,
        t7.connid,
        sclient,
        dclient,
        sversion sversion,
        dversion dversion
    from
    (
        select  
            nvl(t5.ip,t6.ip) ip,
            nvl(t5.streamid,t6.streamid) streamid,
            nvl(t5.connid,t6.connid) connid
        from
        (
            select  
                t1.*
            from
            (
                select
                    ip,
                    streamid,
                    split(connid,'_')[1] connid,
                    logtime
                from ${APP}.dwd_backend_licode_publish_erizo
                where dt='$do_date'
                group by ip, streamid, split(connid,'_')[1], logtime
            )t1
            left join
            (
                select
                    streamid
                from ${APP}.dwd_backend_licode_publish_initializing
                where dt='$do_date'
                group by streamid
            )t2
            on t1.streamid=t2.streamid
            where t1.streamid is not null and t2.streamid is null
        )t5
        full outer join
        (
            select 
                t3.*
            from
            (
                select
                    ip,
                    streamid,
                    split(connid,'_')[1] connid
                from ${APP}.dwd_backend_licode_subscribe_start 
                where dt='$do_date'
                group by ip, streamid, split(connid,'_')[1]
            )t3
            left join
            (
                select
                    streamid
                from ${APP}.dwd_backend_licode_subscribe_p2p 
                where dt='$do_date'
                group by streamid
            )t4
            on t3.streamid=t4.streamid
            where t3.streamid is not null and t4.streamid is null
        )t6
        on t5.streamid=t6.streamid
        where t5.streamid is null or t6.streamid is null
    )t7
    left join
    (
        select
            connid,sclient,dclient,sversion,dversion
        from ${APP}.dwd_new_connect
        where dt='$do_date'
    )t8
    on t7.connid=t8.connid
)t11
left join
(
   select
        streamid
    from ${APP}.dwd_backend_licode_all_ready
    where dt='$do_date'
)t12
on t11.streamid=t12.streamid
group by sclient,dclient,t11.sversion,t11.dversion
order by sclient,dclient,t11.sversion desc,t11.dversion desc;"

dws_client_session_day="
insert overwrite table ${APP}.dws_client_session_day partition (dt='$do_date')
select
    sclient,
    dclient,
    sum(if(handshake_success_connid is not null,1,0)) c1,
    sum(if(authentication_success_connid is not null,1,0)) c1,
    count(1) c2,
    cast(sum(if(handshake_success_connid is not null,1,0)) /count(t1.connid) * 100 as decimal(16,2)),
    cast(sum(if(authentication_success_connid is not null,1,0)) /count(t1.connid) * 100 as decimal(16,2))
from
(
    select connid,
           case
               when sclient='1' then 'PC'
               when sclient='2' then 'MAC'
               when sclient='3' then 'IOS'
               when sclient='4' then 'ANDROID'
               when sclient='5' then 'LINUX'
               when sclient='6' then 'ONTHER'
            else sclient end sclient,
           case
               when dclient='1' then 'PC'
               when dclient='2' then 'MAC'
               when dclient='3' then 'IOS'
               when dclient='4' then 'ANDROID'
               when dclient='5' then 'LINUX'
               when dclient='6' then 'ONTHER'
            else dclient end dclient
    from ${APP}.dwd_new_connect
    where dt='$do_date'
)t1
left join
(
    select
           connid authentication_success_connid
    from ${APP}.dwd_authentication_success
    where dt='$do_date'
    group by connid
)t2
on t1.connid=t2.authentication_success_connid
left join
(
    select
           connid handshake_success_connid
    from ${APP}.dwd_handshake_success
    where dt='$do_date'
    group by connid
)t3
on t1.connid=t3.handshake_success_connid
group by t1.sclient,t1.dclient
order by sclient desc,dclient desc;"

dws_province_isp_client_day_total="
insert overwrite table ${APP}.dws_province_isp_client_day_total partition (dt='$do_date')
select
    sprovince,
    su_isp,
    s_client,
    dprovince,
    du_isp,
    d_client,
    relay_server_name,
    customer_type,
    sum(if(handshake_success_connid is not null,1,0)) handshake_success_count,
    sum(if(authentication_success_connid is not null,1,0)) authentication_success_count,
    count(1) new_connect_count,
    cast(sum(if(handshake_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) handshake_success_rate,
    cast(sum(if(authentication_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) authentication_success_rate,
    cast(avg((\`out\`-\`in\`) / link_time) AS DECIMAL(16,2) ) bitrate
from
 (
    select
           connid,
           sprovince,
           dprovince,
	   relay_server_name,
           case
               when enterprise='0' then 'customer'
               when enterprise='1' then 'business'
           else 'other' end customer_type,
           case
               when sisp='联通' then '联通'
               when sisp='移动' then '移动'
               when sisp='电信' then '电信'
               when sline='鹏博士'  then '鹏博士'  
           else 'other' end su_isp,
            case
               when disp='联通' then '联通'
               when disp='移动' then '移动'
               when disp='电信' then '电信'
               when dline='鹏博士'  then '鹏博士'  
           else 'other' end du_isp,
            case
               when sclient='1' then 'PC'
               when sclient='2' then 'MAC'
               when sclient='3' then 'IOS'
               when sclient='4' then 'ANDROID'
               when sclient='5' then 'LINUX'
               when sclient='6' then 'ONTHER'
	       when sclient='9' then 'MOBILE_OHOS'
               when sclient='10' then 'PC_OHOS'
            else sclient end s_client,
           case
               when dclient='1' then 'PC'
               when dclient='2' then 'MAC'
               when dclient='3' then 'IOS'
               when dclient='4' then 'ANDROID'
               when dclient='5' then 'LINUX'
               when dclient='6' then 'ONTHER'
	       when sclient='9' then 'MOBILE_OHOS'
               when sclient='10' then 'PC_OHOS'
            else dclient end d_client
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and sprovince in ('广东省','江苏省','上海市','浙江省','福建省','山东省','河南省','四川省','北京市','河北省','辽宁省','吉林省','湖南省','安徽省','湖北省','江西省','广西壮族自治区','黑龙江省','陕西省','山西省','重庆市','贵州省','天津市','云南省','内蒙古自治区','新疆维吾尔自治区','甘肃省','中国香港','海南省','宁夏回族自治区','中国台湾','青海省','西藏自治区','澳门特别行政区')
    and dprovince in ('广东省','江苏省','上海市','浙江省','福建省','山东省','河南省','四川省','北京市','河北省','辽宁省','吉林省','湖南省','安徽省','湖北省','江西省','广西壮族自治区','黑龙江省','陕西省','山西省','重庆市','贵州省','天津市','云南省','内蒙古自治区','新疆维吾尔自治区','甘肃省','中国香港','海南省','宁夏回族自治区','中国台湾','青海省','西藏自治区','澳门特别行政区')
    and enterprise in ('0','1')
)t1
left join
(
    select
           connid authentication_success_connid
    from ${APP}.dwd_authentication_success
    where dt='$do_date'
    group by connid
)t2
on t1.connid=t2.authentication_success_connid
left join
(
    select
        connid,
        \`out\`,
        \`in\`,
        link_time
    from ${APP}.dwd_session_success
    where dt='$do_date'
    and \`out\` <> 0
    group by connid,\`out\`,\`in\`,link_time
)t3
on t1.connid=t3.connid
left join
(
    select
        connid handshake_success_connid
    from ${APP}.dwd_handshake_success
    where dt='$do_date'
    group by connid
)t4
on t1.connid=t4.handshake_success_connid
group by sprovince, su_isp, s_client, dprovince, du_isp, d_client,relay_server_name, customer_type
order by sprovince ,su_isp desc,s_client desc,dprovince,d_client,relay_server_name desc;"

dws_backend_licode_day_total="
insert overwrite table ${APP}.dws_backend_licode_day_total partition (dt='$do_date')
select
    n_id,hd,au,ec_hd,ec_su,t10.str,if(t10.ea_err is null ,'6',ea_err)
from
(
    select
        n_id,hd,au,ec_hd,if(ec_su is null,'4',ec_su) ec_su
    from
    (
        select
            n_id, hd, au , if(ec_hd is null,'4',ec_hd) ec_hd
        from
        (
            select
                n_id,
                hd,
                if(t4.a_id is not null ,'1','0') au
            from
            (
                select
                    t1.connid n_id,
                    if(t2.connid is not null ,'1','0') hd
                from
                (
                    select
                        connid,
                        ts
                    from ${APP}.dwd_new_connect
                    where dt='$do_date'
                    and sclient='PC'
                    and dclient='PC'
                    and action='2'
                )t1
                left join
                (
                    select
                        connid
                    from ${APP}.dwd_handshake_success
                    where dt='$do_date'
                    group by connid
                )t2
                on t1.connid=t2.connid
            )t3
            left join
            (
                select
                    connid a_id
                from ${APP}.dwd_authentication_success
                where dt='$do_date'
                group by connid
            )t4
            on t3.n_id=t4.a_id
        )t5
        left join
        (
            select
                nvl(t1.conn_id,t2.conn_id) id,
                case
                    when t1.conn_id is not null and t2.conn_id is not null then '1'
                    when t1.conn_id is null and t2.conn_id is not null then '2'
                    when t1.conn_id is not null and t2.conn_id is null then '3'
                end ec_hd
            from
             (
                select
                    conn_id
                from ${APP}.dwd_backend_licode_newconnect
                where dt='$do_date'
                and type='viewer'
                group by conn_id
             )t1
            full outer join
            (
               select
                    conn_id
                from ${APP}.dwd_backend_licode_newconnect
                where dt='$do_date'
                and type='presenter'
                group by conn_id
            )t2
            on t1.conn_id=t2.conn_id
        )t6
        on t5.n_id=t6.id
    )t7
    left join
    (
        select
            nvl(t1.conn_id,t2.conn_id) id,
                case
                    when t1.conn_id is not null and t2.conn_id is not null then '1'
                    when t1.conn_id is null and t2.conn_id is not null then '2'
                    when t1.conn_id is not null and t2.conn_id is null then '3'
                end ec_su
        from
         (
            select
                split(connid,'_')[1] conn_id
             from ${APP}.dwd_backend_licode_subscribe_start
             where dt='$do_date'
             group by split(connid,'_')[1]
         )t1
        full outer join
         (
            select
                split(connid,'_')[1] conn_id
             from ${APP}.dwd_backend_licode_publish_erizo
             where dt='$do_date'
             group by split(connid,'_')[1]
         )t2
        on t1.conn_id=t2.conn_id
    )t8
    on t7.n_id=t8.id
)t9
left join
(
    select
        nvl(t1.conn_id,t2.conn_id) id,
        nvl(t1.sub_str,t2.pub_str) str,
        case
            when t1.sub_str is null and t2.pub_str is not null then '2'
            when t1.sub_str is not null and t2.pub_str is null then '3'
    end ea_err
    from
    (
        select
            streamid sub_str,
            split(connid,'_')[1] conn_id
        from ${APP}.dwd_backend_licode_addsubscriber_ready
        where dt ='$do_date'
        and streamid is not null
        group by streamid, split(connid,'_')[1]
    )t1
    full outer join
    (
        select
            streamid pub_str,split(connid,'_')[1] conn_id
        from ${APP}.dwd_backend_licode_addPublisher_ready
        where dt ='$do_date'
        and streamid is not null
        group by streamid,split(connid,'_')[1]
    )t2
    on t1.sub_str=t2.pub_str
    where t1.sub_str is null or t2.pub_str is null
    union all
    select
        nvl(t3.conn_id,t4.conn_id),
        nvl(t3.sub_str,t4.stream_id),
        case
            when t3.conn_id is not null and t4.conn_id is null then '1'
            when t3.conn_id is null and t4.conn_id is not null then '4'
            when t3.conn_id is not null and t4.conn_id is not null then '5'
        end ea_ok
    from(
        select
            t1.conn_id,
            t1.sub_str
        from
        (
            select
                streamid sub_str,
                split(connid,'_')[1] conn_id
            from ${APP}.dwd_backend_licode_addsubscriber_ready
            where dt ='$do_date'
            and streamid is not null
            group by streamid, split(connid,'_')[1]
        )t1
        inner join
        (
            select
                streamid pub_str,split(connid,'_')[1] conn_id
            from ${APP}.dwd_backend_licode_addPublisher_ready
            where dt ='$do_date'
            and streamid is not null
            group by streamid,split(connid,'_')[1]
        )t2
        on t1.sub_str=t2.pub_str
    )t3
    full outer join
    (
        select
            conn_id,
            stream_id
        from ${APP}.dwd_sdk_su
        where dt='$do_date'
        and conn_type='1' and cast(first_frame_time as bigint) > 0
        and success_num=over_num
        group by conn_id,stream_id
    )t4
    on t3.conn_id=t4.conn_id
)t10
on t9.n_id=t10.id;"

dws_backend_licode_day_total_4500="
insert overwrite table ${APP}.dws_backend_licode_day_total_4500 partition (dt='$do_date')
select
    n_id,hd,au,ec_hd,ec_su,t10.str,if(t10.ea_err is null ,'6',ea_err)
from
(
    select
        n_id,hd,au,ec_hd,if(ec_su is null,'4',ec_su) ec_su
    from
    (
        select
            n_id, hd, au , if(ec_hd is null,'4',ec_hd) ec_hd
        from
        (
            select
                n_id,
                hd,
                if(t4.a_id is not null ,'1','0') au
            from
            (
                select
                    t1.connid n_id,
                    if(t2.connid is not null ,'1','0') hd
                from
                (
                    select
                        connid,
                        ts
                    from ${APP}.dwd_new_connect
                    where dt='$do_date'
                    and sclient='PC'
                    and dclient='PC'
                    and ((action='2' and labtype='logic') OR (action='true' and labtype='excalibur'))
                    and sversion >='4.5.0.0'
                    and dversion >='4.5.0.0'
                )t1
                left join
                (
                    select
                        connid
                    from ${APP}.dwd_handshake_success
                    where dt='$do_date'
                    group by connid
                )t2
                on t1.connid=t2.connid
            )t3
            left join
            (
                select
                    connid a_id
                from ${APP}.dwd_authentication_success
                where dt='$do_date'
                group by connid
            )t4
            on t3.n_id=t4.a_id
        )t5
        left join
        (
            select
                nvl(t1.conn_id,t2.conn_id) id,
                case
                    when t1.conn_id is not null and t2.conn_id is not null then '1'
                    when t1.conn_id is null and t2.conn_id is not null then '2'
                    when t1.conn_id is not null and t2.conn_id is null then '3'
                end ec_hd
            from
             (
                select
                    conn_id
                from ${APP}.dwd_backend_licode_newconnect
                where dt='$do_date'
                and type='viewer'
                group by conn_id
             )t1
            full outer join
            (
               select
                    conn_id
                from ${APP}.dwd_backend_licode_newconnect
                where dt='$do_date'
                and type='presenter'
                group by conn_id
            )t2
            on t1.conn_id=t2.conn_id
        )t6
        on t5.n_id=t6.id
    )t7
    left join
    (
        select
            nvl(t1.conn_id,t2.conn_id) id,
                case
                    when t1.conn_id is not null and t2.conn_id is not null then '1'
                    when t1.conn_id is null and t2.conn_id is not null then '2'
                    when t1.conn_id is not null and t2.conn_id is null then '3'
                end ec_su
        from
         (
            select
                split(connid,'_')[2] conn_id
             from ${APP}.dwd_backend_licode_subscribe_start
             where dt='$do_date'
             group by split(connid,'_')[2]
         )t1
        full outer join
         (
            select
                split(connid,'_')[2] conn_id
             from ${APP}.dwd_backend_licode_publish_erizo
             where dt='$do_date'
             group by split(connid,'_')[2]
         )t2
        on t1.conn_id=t2.conn_id
    )t8
    on t7.n_id=t8.id
)t9
left join
(
    select
        nvl(t1.conn_id,t2.conn_id) id,
        nvl(t1.sub_str,t2.pub_str) str,
        case
            when t1.sub_str is null and t2.pub_str is not null then '2'
            when t1.sub_str is not null and t2.pub_str is null then '3'
    end ea_err
    from
    (
        select
            streamid sub_str,
            split(connid,'_')[2] conn_id
        from ${APP}.dwd_backend_licode_addsubscriber_ready
        where dt ='$do_date'
        and streamid is not null
        group by streamid, split(connid,'_')[2]
    )t1
    full outer join
    (
        select
            streamid pub_str,split(connid,'_')[2] conn_id
        from ${APP}.dwd_backend_licode_addPublisher_ready
        where dt ='$do_date'
        and streamid is not null
        group by streamid,split(connid,'_')[2]
    )t2
    on t1.sub_str=t2.pub_str
    where t1.sub_str is null or t2.pub_str is null
    union all
    select
        nvl(t3.conn_id,t4.conn_id),
        nvl(t3.sub_str,t4.stream_id),
        case
            when t3.conn_id is not null and t4.conn_id is null then '1'
            when t3.conn_id is null and t4.conn_id is not null then '4'
            when t3.conn_id is not null and t4.conn_id is not null then '5'
        end ea_ok
    from(
        select
            t1.conn_id,
            t1.sub_str
        from
        (
            select
                streamid sub_str,
                split(connid,'_')[2] conn_id
            from ${APP}.dwd_backend_licode_addsubscriber_ready
            where dt ='$do_date'
            and streamid is not null
            group by streamid, split(connid,'_')[2]
        )t1
        inner join
        (
            select
                streamid pub_str,split(connid,'_')[2] conn_id
            from ${APP}.dwd_backend_licode_addPublisher_ready
            where dt ='$do_date'
            and streamid is not null
            group by streamid,split(connid,'_')[2]
        )t2
        on t1.sub_str=t2.pub_str
    )t3
    full outer join
    (
        select
            stream_id ,split(conn_id,'_')[2] conn_id
        from ${APP}.dwd_backend_licode_sdk_report
        where dt ='$do_date'
        and reason='1'
        group by stream_id ,split(conn_id,'_')[2]
    )t4
    on t3.conn_id=t4.conn_id and t3.sub_str=t4.stream_id
)t10
on t9.n_id=t10.id;"

dws_backend_ec_error_total="
insert overwrite table ${APP}.dws_backend_ec_error_total partition (dt='$do_date')
select   
    conn_id,'loss data' flag
from ${APP}.dws_backend_licode_day_total
where dt='$do_date'
and forward_authentication_success='1'
and ec_success <> '1'
and ea_success in ('1','5')
group by conn_id
union all 
select
    total_connid,
    case when subend_connid is not null then 'viewer_loss data'
         when subend_connid is null and disconnect_connid is not null  then concat_ws('_',type,reason)
    else 'other' end reason
from
(
    select
        t4.conn_id total_connid,t5.conn_id subend_connid
    from
    (
        select
            t1.conn_id,ec_handshake_success,ec_success
        from
        (
            select
                conn_id,ec_handshake_success,ec_success
            from ${APP}.dws_backend_licode_day_total
            where dt='$do_date'
            and forward_authentication_success='1'
            and ec_success <> '1'
            group by conn_id,ec_handshake_success,ec_success
        )t1
        left join
        (
            select
                conn_id
            from ${APP}.dws_backend_licode_day_total
            where dt='$do_date'
            and forward_authentication_success='1'
            and ec_success <> '1'
            and ea_success in ('1','5')
            group by conn_id
        )t2
        on t1.conn_id=t2.conn_id
        where t2.conn_id is null and ec_success='2' and ec_handshake_success='1'
    )t4
    left join
    (
        select
            split(conn_id,'_')[1] conn_id
        from ${APP}.dwd_backend_licode_subscribe_end
        where dt='$do_date'
        group by split(conn_id,'_')[1]
    )t5
    on t4.conn_id=t5.conn_id
)t3
left join
(
    select
        conn_id disconnect_connid,type,reason
    from ${APP}.dwd_backend_licode_socket_disconnected
    where dt='$do_date'
    and type='viewer'
    group by conn_id, type, reason
)t4
on t3.total_connid=t4.disconnect_connid
union all  
select
    total_connid,
    case
         when t10.conn_id is not null then 'viewer_loss data'
         when t10.conn_id is null and turndata_connected='1' and flag='1' then 'viewer_loss data'
         when t10.conn_id is null and turndata_connected='0' and flag='1' and isp=serv then 'viewer_isp same failed'
         when t10.conn_id is null and turndata_connected='0' and flag='1' and isp<>serv then 'viewer_isp diff failed'
    else 'viewer_no log' end
from
(
    select
        conn_id total_connid,turndata_connected,isp,serv,flag
    from
    (
        select
            conn_id, suid, isp, ip, ts, serv, su_sig_id, su_sig_ip, su_sig_ts, turndata_connected,flag, first_value(flag) over (partition by conn_id order by flag asc ) flag2, first_value(flag) over (partition by conn_id  order by flag desc ) flag3
        from
         (
            select 
                conn_id, suid, isp, ip, ts, serv, su_sig_id, su_sig_ip, su_sig_ts, turndata_connected,if(su_sig_ts-ts between 0 and 10000,1,0) flag
            from
            (
                select
                    t3.conn_id,t4.suid,t4.isp,t4.ip,t4.ts,t4.serv
                from
                (
                    select
                        t1.conn_id,ec_handshake_success,ec_success
                    from
                    (
                        select
                            conn_id,ec_handshake_success,ec_success
                        from ${APP}.dws_backend_licode_day_total
                        where dt='$do_date'
                        and forward_authentication_success='1'
                        and ec_success <> '1'
                        group by conn_id,ec_handshake_success,ec_success
                    )t1
                    left join
                    (
                        select
                            conn_id
                        from ${APP}.dws_backend_licode_day_total
                        where dt='$do_date'
                        and forward_authentication_success='1'
                        and ec_success <> '1'
                        and ea_success in ('1','5')
                        group by conn_id
                    )t2
                    on t1.conn_id=t2.conn_id
                    where t2.conn_id is null and ec_success='2' and ec_handshake_success<>'1'
                )t3
                left join
                (
                    select
                        connid,suid,split(split(relay_server,'-')[0],':')[0] ip,ts ,case when sisp='中国移动' or sisp='移动' then '移动' when sisp='中国电信' or sisp='电信' then '电信' when sisp='中国联通' or sisp='联通'  then '联通' else 'other' end isp,split(relay_server_name,'-')[2] serv
                    from ${APP}.dwd_new_connect
                    where dt='$do_date'
                )t4
                on t3.conn_id=t4.connid
            )t5
            left join
            (
                select
                    id su_sig_id,split(split(turndata_url,'//')[1],':')[0] su_sig_ip,turndata_begintime su_sig_ts,turndata_connected
                from ${APP}.dwd_licode_sig
                where dt='$do_date'
                and clientype='client'
                and turndata_connected in ('0','1')
            )t6
            on t5.suid=t6.su_sig_id and t5.ip=t6.su_sig_ip
        )t7
    )t8
    where (flag2 = flag3) or (flag != flag2 and flag = 1)
    group by conn_id,turndata_connected,isp,serv,flag
)t9
left join
(
    select
        split(conn_id,'_')[1] conn_id
    from ${APP}.dwd_backend_licode_subscribe_end
    where dt='$do_date'
    group by split(conn_id,'_')[1]
)t10
on t9.total_connid=t10.conn_id
union all 
select
    total_connid,
    case when pubend_connid is not null then 'presenter_loss data'
         when pubend_connid is null and disconnect_connid is not null  then concat_ws('_',type,reason)
    else 'other' end reason
from
(
    select
        t3.conn_id total_connid,type,reason,disconnect_connid
    from
    (
        select
            t1.conn_id,ec_handshake_success,ec_success
        from
        (
            select
                conn_id,ec_handshake_success,ec_success
            from ${APP}.dws_backend_licode_day_total
            where dt='$do_date'
            and forward_authentication_success='1'
            and ec_success <> '1'
            group by conn_id,ec_handshake_success,ec_success
        )t1
        left join
        (
            select
                conn_id
            from ${APP}.dws_backend_licode_day_total
            where dt='$do_date'
            and ec_success <> '1'
            and forward_authentication_success='1'
            and ea_success in ('1','5')
            group by conn_id
        )t2
        on t1.conn_id=t2.conn_id
        where t2.conn_id is null and ec_success='3' and ec_handshake_success='1'
    )t3
    left join
    (
        select
            conn_id disconnect_connid,type,reason
        from ${APP}.dwd_backend_licode_socket_disconnected
        where dt='$do_date'
        and type='presenter'
        group by conn_id, type, reason
    )t4
    on t3.conn_id=t4.disconnect_connid
)t5
left join
(
     select
        split(conn_id,'_')[1] pubend_connid
    from ${APP}.dwd_backend_licode_publish_end
    where dt='$do_date'
    group by split(conn_id,'_')[1]
)t6
on t5.total_connid=t6.pubend_connid
union all 
select
    conn_id,
    case
         when pubend_connid is not null then 'presenter_loss data'
         when pubend_connid is null and turndata_connected='1' and flag = '1' then 'presenter_loss data'
         when pubend_connid is null and turndata_connected='0' and flag = '1' and isp=serv then 'presenter_isp same failed'
         when pubend_connid is null and turndata_connected='0' and flag = '1' and isp<>serv then 'presenter_isp diff failed'
    else 'presenter_no log' end
from
(
    select
        conn_id,turndata_connected,isp,serv,flag
    from
    (
        select
            conn_id, duid, isp, ip, ts, serv, du_sig_id,flag, du_sig_ip, du_sig_ts, turndata_connected, first_value(flag) over (partition by conn_id order by flag asc ) flag2, first_value(flag) over (partition by conn_id  order by flag desc ) flag3
        from
        (
            select 
                conn_id, duid, isp, ip, ts, serv, du_sig_id, du_sig_ip, du_sig_ts, turndata_connected,if(du_sig_ts-ts between 0 and 10000,1,0) flag
            from
            (
                select
                    t3.conn_id,t4.duid,t4.isp,t4.ip,t4.ts,t4.serv
                from
                (
                    select
                        t1.conn_id,ec_handshake_success,ec_success
                    from
                    (
                        select
                            conn_id,ec_handshake_success,ec_success
                        from ${APP}.dws_backend_licode_day_total
                        where dt='$do_date'
                        and forward_authentication_success='1'
                        and ec_success <> '1'
                        group by conn_id,ec_handshake_success,ec_success
                    )t1
                    left join
                    (
                        select
                            conn_id
                        from ${APP}.dws_backend_licode_day_total
                        where dt='$do_date'
                        and ec_success <> '1'
                        and forward_authentication_success='1'
                        and ea_success in ('1','5')
                        group by conn_id
                    )t2
                    on t1.conn_id=t2.conn_id
                    where t2.conn_id is null and ec_success='3' and ec_handshake_success<>'1'
                )t3
                left join
                (
                    select
                        connid,duid,split(split(relay_server,'-')[1],':')[0] ip,ts ,case when disp='中国移动' or disp='移动' then '移动' when disp='中国电信' or disp='电信' then '电信' when disp='中国联通' or disp='联通'  then '联通' else 'other' end isp,split(relay_server_name,'-')[5] serv
                    from ${APP}.dwd_new_connect
                    where dt='$do_date'
                )t4
                on t3.conn_id=t4.connid
            )t5
            left join
            (
                select
                    id du_sig_id,split(split(turndata_url,'//')[1],':')[0] du_sig_ip,turndata_begintime du_sig_ts,turndata_connected
                from ${APP}.dwd_licode_sig
                where dt='$do_date'
                and clientype='host'
                and turndata_connected in ('0','1')
            )t6
            on t5.duid=t6.du_sig_id and t5.ip=t6.du_sig_ip
        )t7
    )t8
    where (flag2 = flag3) or (flag != flag2 and flag = 1)
    group by conn_id,turndata_connected,isp,serv,flag
)t9
left join
(
    select
        split(conn_id,'_')[1] pubend_connid
    from ${APP}.dwd_backend_licode_publish_end
    where dt='$do_date'
    group by split(conn_id,'_')[1]
)t10
on t9.conn_id=t10.pubend_connid
union all  
select  conn_id,concat_ws('-',su,du) from (
    select
        conn_id,
        case
                 when subend_connid is not null then 'viewer_loss data'
                 when subend_connid is null and su_connect='1' and su_flag='1' then 'viewer_loss data'
                 when subend_connid is null and su_connect='0' and su_flag='1' and sisp=serv then 'viewer_isp same failed'
                 when subend_connid is null and su_connect='0' and su_flag='1' and sisp<>serv then 'viewer_isp diff failed'
        else 'viewer_no log' end su,
        case
                 when pubend_connid is not null then 'presenter_loss data'
                 when pubend_connid is null and du_connect='1' and du_flag='1' then 'presenter_loss data'
                 when pubend_connid is null and du_connect='0' and du_flag='1' and disp=dserv then 'presenter_isp same failed'
                 when pubend_connid is null and du_connect='0' and du_flag='1' and disp<>dserv then 'presenter_isp diff failed'
        else 'presenter_no log' end du
    from
     (
        select
        conn_id, su_connect, du_connect, sisp, serv, disp, dserv, su_flag, du_flag, subend_connid
    from
     (
        select
            conn_id,su_connect,du_connect,sisp,serv,disp,dserv,su_flag,du_flag
        from
              (select *, first_value(du_flag) over (partition by conn_id  order by du_flag asc ) flag2, first_value(du_flag) over (partition by conn_id  order by du_flag desc ) flag3
        from (
            select

            conn_id,  duid, sisp, duip, ts, serv, disp, dserv, su_sig_id, su_sig_ip, su_connect,if(du_sig_ts-ts between 0 and 10000,1,0) du_flag,du_connect,su_flag
            from
            (
                select
                    conn_id,  duid, sisp, duip, ts, serv, disp, dserv, su_sig_id, su_sig_ip, su_connect,su_flag
                from
                (
                    select
                        conn_id, duid,duip, sisp, su_sig_ip,disp, ts, serv,dserv, su_sig_id, su_connect,su_flag, first_value(su_flag) over (partition by conn_id  order by su_flag asc ) flag2, first_value(su_flag) over (partition by conn_id  order by su_flag desc ) flag3
                    from
                    (
                        select
                            conn_id, suid, duid, suip, duip, ts, sisp, serv, disp, dserv, su_sig_id, su_sig_ip,  su_connect ,if(su_sig_ts-ts between 0 and 10000,1,0) su_flag
                        from
                        (
                            select
                                 conn_id, suid, duid, suip, duip, ts, sisp, serv, disp, dserv
                            from
                            (
                                select
                                    t1.conn_id,ec_handshake_success,ec_success
                                from
                                (
                                    select
                                        conn_id,ec_handshake_success,ec_success
                                    from ${APP}.dws_backend_licode_day_total
                                    where dt='$do_date'
                                    and forward_authentication_success='1'
                                    and ec_success <> '1'
                                    group by conn_id,ec_handshake_success,ec_success
                                )t1
                                left join
                                (
                                    select
                                        conn_id
                                    from ${APP}.dws_backend_licode_day_total
                                    where dt='$do_date'
                                    and ec_success <> '1'
                                    and forward_authentication_success='1'
                                    and ea_success in ('1','5')
                                    group by conn_id
                                )t2
                                on t1.conn_id=t2.conn_id
                                where t2.conn_id is null and ec_success='4' and ec_handshake_success='4'
                            )t3
                            left join
                            (
                                select
                                    connid,suid,duid,split(split(relay_server,'-')[0],':')[0] suip,split(split(relay_server,'-')[1],':')[0] duip,ts ,
                                    case when sisp='中国移动' or sisp='移动' then '移动' when sisp='中国电信' or sisp='电信' then '电信' when sisp='中国联通' or sisp='联通'  then '联通' else 'other' end sisp,split(relay_server_name,'-')[2] serv,
                                    case when disp='中国移动' or disp='移动' then '移动' when disp='中国电信' or disp='电信' then '电信' when disp='中国联通' or disp='联通'  then '联通' else 'other' end disp,split(relay_server_name,'-')[5] dserv
                                from ${APP}.dwd_new_connect
                                where dt='$do_date'
                            )t4
                            on  t3.conn_id=t4.connid
                        )t5
                        left join
                        (
                            select
                                id su_sig_id,split(split(turndata_url,'//')[1],':')[0] su_sig_ip,turndata_begintime su_sig_ts,turndata_connected su_connect
                            from ${APP}.dwd_licode_sig
                            where dt='$do_date'
                            and clientype='client'
                            and turndata_connected in ('0','1')
                        )t6
                        on t5.suid=t6.su_sig_id and t5.suip=t6.su_sig_ip
                    )t7
                )t0
                where (flag2 = flag3) or (su_flag != flag2 and su_flag = 1)
                group by conn_id,  duid, sisp, duip, ts, serv, disp, dserv, su_sig_id, su_sig_ip, su_connect,su_flag
            )t7
            left join
            (
                select
                    id du_sig_id,split(split(turndata_url,'//')[1],':')[0] du_sig_ip,turndata_begintime du_sig_ts,turndata_connected du_connect
                from ${APP}.dwd_licode_sig
                where dt='$do_date'
                and clientype='host'
                and turndata_connected in ('0','1')
            )t8
            on t7.duid=t8.du_sig_id and t7.duip=t8.du_sig_ip

        )t9
        )t10
        where (flag2 = flag3) or (du_flag != flag2 and du_flag = 1)
        group by conn_id,su_connect,du_connect,sisp,serv,disp,dserv,su_flag,du_flag
     )t11
    left join
     (
         select
            split(conn_id,'_')[1] subend_connid
        from ${APP}.dwd_backend_licode_subscribe_end
        where dt='$do_date'
        group by split(conn_id,'_')[1]
     )t12
    on t11.conn_id=t12.subend_connid
     )t13
    left join
     (
         select
            split(conn_id,'_')[1] pubend_connid
        from ${APP}.dwd_backend_licode_publish_end
        where dt='$do_date'
        group by split(conn_id,'_')[1]
     )t14
    on t13.conn_id=t14.pubend_connid
)tt1
union all  
select
    total_connid,concat_ws('-',su,du)
from
(
    select
        total_connid,
        case
             when subend_connid is not null then 'viewer_loss data'
             when subend_connid is null and su_connect='1' and flag='1' then 'viewer_loss data'
             when subend_connid is null and su_connect='0' and flag='1' and sisp=serv then 'viewer_isp same failed'
             when subend_connid is null and su_connect='0' and flag='1' and sisp<>serv then 'viewer_isp diff failed'
        else 'viewer_no log' end su,
        case when pubend_connid is not null then 'presenter_loss data'
             when pubend_connid is null and disconnect_id is not null  then concat_ws('_',type,reason)
        else 'other' end du
    from
     (
        select
        total_connid, suid, suip, ts, sisp, serv, su_sig_id, su_sig_ip, su_sig_ts, su_connect, flag, disconnect_id, type, reason, subend_connid
    from
         (
            select
            total_connid, suid, suip, ts, sisp, serv, su_sig_id, su_sig_ip, su_sig_ts, su_connect, flag, disconnect_id, type, reason
        from (
                 select total_connid, suid, suip, ts, sisp, serv, su_sig_id, su_sig_ip, su_sig_ts,su_connect,flag
                 from (select total_connid, suid, suip, ts, sisp, serv, su_sig_id, su_sig_ip, su_sig_ts,su_connect,flag,
                              first_value(flag) over (partition by total_connid order by flag asc )  flag2,
                              first_value(flag) over (partition by total_connid order by flag desc ) flag3
                       from (
                                select total_connid,
                                       suid,
                                       suip,
                                       ts,
                                       sisp,
                                       serv,
                                       su_sig_id,
                                       su_sig_ip,
                                       su_sig_ts,
                                       if(su_sig_ts - ts between 0 and 10000, 1, 0) flag,
                                       su_connect
                                from (
                                         select conn_id total_connid,
                                                suid,
                                                suip,
                                                ts,
                                                sisp,
                                                serv
                                         from (
                                                  select t1.conn_id,
                                                         ec_handshake_success,
                                                         ec_success
                                                  from (
                                                           select conn_id,
                                                                  ec_handshake_success,
                                                                  ec_success
                                                           from ${APP}.dws_backend_licode_day_total
                                                           where dt = '$do_date'
                                                             and forward_authentication_success = '1'
                                                             and ec_success <> '1'
                                                           group by conn_id, ec_handshake_success, ec_success
                                                       ) t1
                                                           left join
                                                       (
                                                           select conn_id
                                                           from ${APP}.dws_backend_licode_day_total
                                                           where dt = '$do_date'
                                                             and ec_success <> '1'
                                                             and forward_authentication_success='1'
                                                             and ea_success in ('1', '5')
                                                           group by conn_id
                                                       ) t2
                                                       on t1.conn_id = t2.conn_id
                                                  where t2.conn_id is null
                                                    and ec_success = '4'
                                                    and ec_handshake_success = '2'
                                              ) t3
                                                  left join
                                              (
                                                  select connid,
                                                         suid,
                                                         split(split(relay_server, '-')[1], ':')[0] suip,
                                                         ts,
                                                         case
                                                             when sisp = '中国移动' or sisp = '移动' then '移动'
                                                             when sisp = '中国电信' or sisp = '电信' then '电信'
                                                             when sisp = '中国联通' or sisp = '联通' then '联通'
                                                             else 'other' end                       sisp,
                                                         split(relay_server_name, '-')[2]           serv
                                                  from ${APP}.dwd_new_connect
                                                  where dt = '$do_date'
                                              ) t4
                                              on t3.conn_id = t4.connid
                                     ) t5
                                         left join
                                     (
                                         select id                                          su_sig_id,
                                                split(split(turndata_url, '//')[1], ':')[0] su_sig_ip,
                                                turndata_begintime                                     su_sig_ts,
                                                turndata_connected                          su_connect
                                         from ${APP}.dwd_licode_sig
                                         where dt = '$do_date'
                                           and clientype = 'client'
                                           and turndata_connected in ('0', '1')
                                     ) t6
                                     on t5.suid = t6.su_sig_id and t5.suip = t6.su_sig_ip
                            ) t7) t11
                 where (flag2 = flag3)
                    or (flag != flag2 and flag = 1)
                 group by total_connid, suid, suip, ts, sisp, serv, su_sig_id, su_sig_ip, su_sig_ts,su_connect,flag
             )t12
            left join
        (
            select
                conn_id disconnect_id,type,reason
            from ${APP}.dwd_backend_licode_socket_disconnected
            where dt='$do_date'
            and type='presenter'
            group by conn_id, type, reason
        )t8
        on t12.total_connid=t8.disconnect_id
     )t9
    left join
     (
        select
            split(conn_id,'_')[1] subend_connid
        from ${APP}.dwd_backend_licode_subscribe_end
        where dt='$do_date'
        group by split(conn_id,'_')[1]
     )t10
    on t9.total_connid=t10.subend_connid
     )t11
    left join
    (
        select
            split(conn_id,'_')[1] pubend_connid
        from ${APP}.dwd_backend_licode_publish_end
        where dt='$do_date'
        group by split(conn_id,'_')[1]
    )t12
    on t11.total_connid=t12.pubend_connid
)t9
union all  
select
    total_connid,concat_ws('-',su,du)
from
(
    select
        total_connid,
        case
                when pubend_connid is not null then 'presenter_loss data'
                 when pubend_connid is null and du_connect='1' and flag='1' then 'presenter_loss data'
                 when pubend_connid is null and du_connect='0' and flag='1' and disp=dserv then 'presenter_isp same failed'
                 when pubend_connid is null and du_connect='0' and flag='1' and disp<>dserv then 'presenter_isp diff failed'
        else 'presenter_no log' end du,
        case when subend_connid is not null then 'viewer_loss data'
             when subend_connid is null and disconnect_connid is not null  then concat_ws('_',type,reason)
        else 'other' end su
    from
     (
         select
            total_connid, duid, duip, ts, disp, dserv, du_sig_id, du_sig_ip, du_sig_ts, du_connect, flag, disconnect_connid, type, reason, subend_connid
        from
         (
            select
                total_connid, duid, duip, ts, disp, dserv, du_sig_id, du_sig_ip, du_sig_ts, du_connect, flag, disconnect_connid, type, reason
            from (
                     select total_connid, duid, duip, ts, disp, dserv, du_sig_id, du_sig_ip, du_sig_ts, du_connect,flag
            from
            (
                select total_connid, duid, duip, ts, disp, dserv, du_sig_id, du_sig_ip, du_sig_ts, du_connect,flag, first_value(flag) over (partition by total_connid order by flag asc )  flag2,
                                  first_value(flag) over (partition by total_connid order by flag desc ) flag3
                from (
                    select
                    total_connid, duid, duip, ts, disp, dserv, du_sig_id, du_sig_ip, du_sig_ts, du_connect,if(du_sig_ts - ts between 0 and 10000, 1, 0) flag
                from
                (
                    select
                         total_connid, duid, duip, ts, disp, dserv
                    from
                    (
                        select
                            t1.conn_id total_connid,ec_handshake_success,ec_success
                        from
                        (
                            select
                                conn_id,ec_handshake_success,ec_success
                            from ${APP}.dws_backend_licode_day_total
                            where dt='$do_date'
                            and forward_authentication_success='1'
                            and ec_success <> '1'
                            group by conn_id,ec_handshake_success,ec_success
                        )t1
                        left join
                        (
                            select
                                conn_id
                            from ${APP}.dws_backend_licode_day_total
                            where dt='$do_date'
                            and ec_success <> '1'
                            and forward_authentication_success='1'
                            and ea_success in ('1','5')
                            group by conn_id
                        )t2
                        on t1.conn_id=t2.conn_id
                        where t2.conn_id is null and ec_success='4' and ec_handshake_success='3'
                    )t3
                    left join
                    (
                        select
                            connid,duid,split(split(relay_server,'-')[1],':')[0] duip,ts ,
                            case when disp='中国移动' or disp='移动' then '移动' when disp='中国电信' or disp='电信' then '电信' when disp='中国联通' or disp='联通'  then '联通' else 'other' end disp,split(relay_server_name,'-')[5] dserv
                        from ${APP}.dwd_new_connect
                        where dt='$do_date'
                    )t4
                    on  t3.total_connid=t4.connid
                )t5
                left join
                (
                    select
                        id du_sig_id,split(split(turndata_url,'//')[1],':')[0] du_sig_ip,turndata_begintime du_sig_ts,turndata_connected du_connect
                    from ${APP}.dwd_licode_sig
                    where dt='$do_date'
                    and clientype='host'
                    and turndata_connected in ('0','1')
                )t6
                on t5.duid=t6.du_sig_id and t5.duip=t6.du_sig_ip
            )t7) t11
                     where (flag2 = flag3)
                        or (flag != flag2 and flag = 1)group by total_connid, duid, duip, ts, disp, dserv, du_sig_id, du_sig_ip, du_sig_ts, du_connect,flag)t12
            left join
            (
                select
                    conn_id disconnect_connid,type,reason
                from ${APP}.dwd_backend_licode_socket_disconnected
                where dt='$do_date'
                and type='viewer'
                group by conn_id, type, reason
            )t8
            on t12.total_connid=t8.disconnect_connid
         )t9
         left join
        (
            select
                split(conn_id,'_')[1] subend_connid
            from ${APP}.dwd_backend_licode_subscribe_end
            where dt='$do_date'
            group by split(conn_id,'_')[1]
        )t10
        on t9.total_connid=t10.subend_connid
     )t11
    left join
     (
         select
            split(conn_id,'_')[1] pubend_connid
        from ${APP}.dwd_backend_licode_publish_end
        where dt='$do_date'
        group by split(conn_id,'_')[1]
     )t12
    on t11.total_connid=t12.pubend_connid
)t9
union all  
select
    id,concat_ws('-',su,du)
from
(
    select
        id,
        case when subend_connid is not null then 'viewer_loss data'
             when subend_connid is null and su_id is not null  then concat_ws('_',su_type,su_reason)
        else 'other' end su,
        case when pubend_connid is not null then 'presenter_loss data'
             when pubend_connid is null and du_id is not null  then concat_ws('_',du_type,du_reason)
        else 'other' end du
    from
     (
        select
        t7.id, su_id, su_type, su_reason, du_id, du_type, du_reason,t8.subend_connid
    from
    (
        select
            t5.id, su_id, su_type, su_reason,t6.du_id, du_type, du_reason
        from
        (
            select
                t3.conn_id id, t4.conn_id su_id, su_type, su_reason
            from
            (
                select
                    t1.conn_id
                from
                (
                    select
                        conn_id,ec_handshake_success,ec_success
                    from ${APP}.dws_backend_licode_day_total
                    where dt='$do_date'
                    and forward_authentication_success='1'
                    and ec_success <> '1'
                    group by conn_id,ec_handshake_success,ec_success
                )t1
                left join
                (
                    select
                        conn_id
                    from ${APP}.dws_backend_licode_day_total
                    where dt='$do_date'
                    and ec_success <> '1'
                    and forward_authentication_success='1'
                    and ea_success in ('1','5')
                    group by conn_id
                )t2
                on t1.conn_id=t2.conn_id
                where t2.conn_id is null and ec_success='4' and ec_handshake_success='1'
            )t3
            left join
            (
                select
                    conn_id,type su_type,reason su_reason
                from ${APP}.dwd_backend_licode_socket_disconnected
                where dt='$do_date'
                and type='viewer'
                group by conn_id, type, reason
            )t4
            on  t3.conn_id=t4.conn_id
        )t5
        left join
        (
            select
                conn_id du_id,type du_type,reason du_reason
            from ${APP}.dwd_backend_licode_socket_disconnected
            where dt='$do_date'
            and type='presenter'
            group by conn_id, type, reason
        )t6
        on t5.id=t6.du_id
        )t7
        left join
        (
            select
                split(conn_id,'_')[1] subend_connid
            from ${APP}.dwd_backend_licode_subscribe_end
            where dt='$do_date'
            group by split(conn_id,'_')[1]
        )t8
        on t7.id=t8.subend_connid
     )t9
     left join
    (
        select
            split(conn_id,'_')[1] pubend_connid
        from ${APP}.dwd_backend_licode_publish_end
        where dt='$do_date'
        group by split(conn_id,'_')[1]
    )t10
    on t9.id=t10.pubend_connid
)t7;"


dws_backend_ea_error_total="
insert overwrite table ${APP}.dws_backend_ea_error_total partition (dt='$do_date')
select
    t13.conn_id,t13.stream_id,
    case
        when flag='unpub' and t14.stream_id is not null then t14.unpub_reason
        when flag='unpub' and t14.stream_id is null then 'unpub'
    else flag end flag1
from
(
    select
        t11.conn_id,
        t11.stream_id,
        long_sub_connid,
        long_pub_connid,
        case
            when su_stream_id is not null then 'loss data'  
            when su_stream_id is null and unsub_ts-sub_ts<=5000 then 'unsub'  
            when su_stream_id is null and (unsub_ts-sub_ts>5000 or unsub_ts is null) and unpub_ts-pub_ts<=5000 then 'unpub' 
            when su_stream_id is null and (unsub_ts-sub_ts>5000 or unsub_ts is null) and (unpub_ts-pub_ts>5000 or unpub_ts is null ) and (t12.conn_id is not null and  t12.\`timestamp\`-t11.sub_ts < 180000) then reason 
        else 'other' end flag
    from
    (
        select
            t9.stream_id,conn_id, sub_ts, unsub_ts, unpub_ts, pub_ts,su_stream_id,long_sub_connid,long_pub_connid
        from
        (
            select
                t7.stream_id,conn_id,sub_ts,unsub_ts,pub_ts,su_stream_id,long_sub_connid,long_pub_connid
            from
            (
                select
                    t5.stream_id,conn_id,sub_ts,unsub_ts,su_stream_id,long_sub_connid
                from
                (
                    select
                        t3.stream_id,conn_id, sub_ts,su_stream_id,long_sub_connid
                        
                    from
                    (
                        select
                            t1.stream_id,conn_id,su_stream_id
                        from
                        (
                            select
                                stream_id,conn_id
                            from ${APP}.dws_backend_licode_day_total
                            where dt='$do_date'
                            and ec_success = '1'
                            and ea_success ='2'
                            group by stream_id,conn_id
                        )t1
                        left join
                        (
                            select
                                stream_id su_stream_id
                            from ${APP}.dwd_sdk_su
                            where dt='$do_date'
                            and conn_type='0'
                            and success_num=over_num
                            and ice_state in ('2','3')
                            group by stream_id
                        )t2
                        on t1.stream_id=t2.su_stream_id
                    )t3
                    left join
                    (
                       select
                            streamid,connid long_sub_connid,\`timestamp\` sub_ts
                       from ${APP}.dwd_backend_licode_subscribe_start
                        where dt='$do_date'
                        group by streamid,connid, \`timestamp\`
                    )t4
                    on t3.stream_id=t4.streamid
                )t5
                left join
                (
                   select
                        stream_id,conn_id unsub_connid,\`timestamp\` unsub_ts
                   from ${APP}.dwd_backend_licode_unsubscribe
                    where dt='$do_date'
                    group by stream_id,\`timestamp\`,conn_id
                )t6
                on t5.stream_id=t6.stream_id and t5.long_sub_connid=t6.unsub_connid
            )t7
            left join
            (
                select
                    streamid,\`timestamp\` pub_ts,connid long_pub_connid
               from ${APP}.dwd_backend_licode_publish_erizo
                where dt='$do_date'
                group by streamid, \`timestamp\`,connid
            )t8
            on t7.stream_id=t8.streamid
        )t9
        left join
        (
            select
                    stream_id unpub_stream_id,\`timestamp\` unpub_ts,conn_id unpub_connid
               from ${APP}.dwd_backend_licode_unpublish
                where dt='$do_date'
                group by stream_id,\`timestamp\`,conn_id
        )t10
        on t9.stream_id=t10.unpub_stream_id and t9.long_pub_connid=t10.unpub_connid
    )t11
    left join
    (
        select
            conn_id,reason,\`timestamp\`
        from ${APP}.dwd_backend_licode_socket_disconnected
        where dt='$do_date'
        and type='viewer'
        group by conn_id,reason,\`timestamp\`
    )t12
    on t11.conn_id=t12.conn_id
)t13
left join
(
    select
        stream_id,conn_id,
        case
            when reason='0' then 'kNormal'
            when reason='1' then 'kToP2P'
            when reason='2' then 'kToRelay'
            when reason='3' then 'kToSFU'
            when reason='4' then 'kIceDisconnect'
            when reason='5' then 'kResetEncodeConfig'
	    when reason='6' then 'kCleanResidualStream'
        else 'other' end unpub_reason
    from ${APP}.dwd_backend_licode_sdk_report
    where dt='$do_date'
    and type='presenter'
    group by stream_id,conn_id, case
            when reason='0' then 'kNormal'
            when reason='1' then 'kToP2P'
            when reason='2' then 'kToRelay'
            when reason='3' then 'kToSFU'
            when reason='4' then 'kIceDisconnect'
            when reason='5' then 'kResetEncodeConfig'
            when reason='6' then 'kCleanResidualStream'
        else 'other' end
)t14
on t13.stream_id=t14.stream_id and t13.long_pub_connid=t14.conn_id
union all 
select
    t9.conn_id,t9.stream_id,
    case
        when flag='unpub' and t10.stream_id is not null then t10.unpub_reason
        when flag='unpub' and t10.stream_id is null then 'unpub'
    else flag end flag1
from
(
    select
        t7.conn_id,t7.stream_id,long_connid,
        case
             when du_stream_id is not null then 'loss data'
             when du_stream_id is null and unpub_ts-pub_ts<=5000 then 'unpub'
             when du_stream_id is null and (unpub_ts-pub_ts>5000 or unpub_ts is null ) and (t8.conn_id is not null and t8.\`timestamp\`-t7.pub_ts < 180000)  then reason
        else 'other' end flag
    from
    (
        select
            t5.stream_id,conn_id,du_stream_id,pub_ts,unpub_ts,long_connid
        from
        (
            select
                t3.stream_id,conn_id,du_stream_id, pub_ts,long_connid
                
            from
            (
                select
                    t1.stream_id,conn_id,du_stream_id
                from
                (
                    select
                        stream_id,conn_id
                    from ${APP}.dws_backend_licode_day_total
                    where dt='$do_date'
                    and ec_success = '1'
                    and ea_success ='3'

                    group by stream_id,conn_id
                )t1
                left join
                (
                    select
                        stream_id du_stream_id
                    from ${APP}.dwd_sdk_du
                    where dt='$do_date'
                    and conn_type='0'
                    and success_num=over_num
                    and ice_state in ('2','3')
                    group by stream_id
                )t2
                on t1.stream_id=t2.du_stream_id
            )t3
            left join
            (
               select
                    streamid,\`timestamp\` pub_ts,connid long_connid
               from ${APP}.dwd_backend_licode_publish_erizo
                where dt='$do_date'
                group by streamid, \`timestamp\`,connid
            )t4
            on t3.stream_id=t4.streamid
        )t5
        left join
        (
           select
                stream_id,\`timestamp\` unpub_ts,conn_id unpub_connid
           from ${APP}.dwd_backend_licode_unpublish
            where dt='$do_date'
            group by stream_id,\`timestamp\` ,conn_id
        )t6
        on t5.stream_id=t6.stream_id and t5.long_connid=t6.unpub_connid
    )t7
    left join
    (
        select
            conn_id,reason,\`timestamp\`
        from ${APP}.dwd_backend_licode_socket_disconnected
        where dt='$do_date'
        and type='presenter'
        group by conn_id,reason,\`timestamp\`
    )t8
    on t7.conn_id=t8.conn_id
)t9
left join
(
    select
        stream_id,conn_id,
        case
            when reason='0' then 'kNormal'
            when reason='1' then 'kToP2P'
            when reason='2' then 'kToRelay'
            when reason='3' then 'kToSFU'
            when reason='4' then 'kIceDisconnect'
            when reason='5' then 'kResetEncodeConfig'
	    when reason='6' then 'kCleanResidualStream'
        else 'other' end unpub_reason
    from ${APP}.dwd_backend_licode_sdk_report
    where dt='$do_date'
    and type='presenter'
    group by stream_id,conn_id, case
            when reason='0' then 'kNormal'
            when reason='1' then 'kToP2P'
            when reason='2' then 'kToRelay'
            when reason='3' then 'kToSFU'
            when reason='4' then 'kIceDisconnect'
            when reason='5' then 'kResetEncodeConfig'
	    when reason='6' then 'kCleanResidualStream'
        else 'other' end
)t10
on t9.stream_id=t10.stream_id and t9.long_connid=t10.conn_id
union all 
select
    conn_id,streamid,concat_ws('-',flag1,flag2)
from
(
    select
        t15.conn_id,t15.streamid,
        case
            when su_flag='presenter_unpub' and t16.stream_id is not null then t16.unpub_reason
            when su_flag='presenter_unpub' and t16.stream_id is null then 'presenter_unpub'
        else su_flag end flag1,
        case
            when du_flag='presenter_unpub' and t16.stream_id is not null then t16.unpub_reason
            when du_flag='presenter_unpub' and t16.stream_id is null then 'presenter_unpub'
        else du_flag end flag2
    from
    (
        select
            t13.conn_id,t13.streamid,sub_long_connid,pub_long_connid,
            case
                when su_stream_id is not null then 'viewer_loss data'
                when su_stream_id is null and unsub_ts-sub_ts<=5000 then 'viewer_unsub'
                when su_stream_id is null and (unsub_ts-sub_ts>5000 or unsub_ts is null) and unpub_ts-pub_ts<=5000 then 'presenter_unpub'
                when su_stream_id is null and (unsub_ts-sub_ts>5000 or unsub_ts is null) and (unpub_ts-pub_ts>5000 or unpub_ts is null ) and (su_reason_conn_id is not null and t13.\`timestamp\`-pub_ts < 180000 is not null)  then concat('viewer_',su_reason)
            else 'other' end su_flag,
            case
                 when du_stream_id is not null then 'presenter_loss data'
                 when du_stream_id is null and unpub_ts-pub_ts<=5000 then 'presenter_unpub'
                 when du_stream_id is null and (unpub_ts-pub_ts>5000 or unpub_ts is null ) and (t14.du_reason_conn_id is not null and t14.\`timestamp\`-t13.pub_ts < 180000)  then concat('presenter_',du_reason)
            else 'other' end du_flag
        from
        (
            select
                conn_id, streamid, pub_ts, sub_ts, su_stream_id, du_stream_id, unsub_ts, unpub_ts, su_reason_conn_id, su_reason,sub_long_connid,pub_long_connid,t12.\`timestamp\`
            from
            (
                select
                    conn_id, streamid, pub_ts, sub_ts, su_stream_id, du_stream_id,  unsub_ts, unpub_ts,sub_long_connid,pub_long_connid
                from
                (
                    select
                        conn_id, streamid, pub_ts, sub_ts, su_stream_id, du_stream_id, stream_id, unsub_ts,sub_long_connid,pub_long_connid
                    from
                    (
                        select
                            conn_id, streamid, pub_ts, sub_ts, su_stream_id, du_stream_id,sub_long_connid,pub_long_connid
                        from
                        (
                            select
                                conn_id, streamid, pub_ts, sub_ts, su_stream_id,sub_long_connid,pub_long_connid
                            from
                            (
                                select
                                    t1.conn_id,streamid,pub_ts,sub_ts,sub_long_connid,pub_long_connid
                                from
                                (
                                    select
                                        conn_id
                                    from ${APP}.dws_backend_licode_day_total
                                    where dt='$do_date'
                                    and ec_success = '1'
                                    and ea_success ='6'
                                    group by conn_id
                                )t1
                                left join
                                (
                                    select
                                        tt.conn_id,tt.streamid,pub_ts,sub_ts,pub_long_connid,sub_long_connid
                                    from
                                    (
                                        select
                                            split(connid,'_')[1] conn_id,streamid,\`timestamp\` pub_ts,connid pub_long_connid
                                        from ${APP}.dwd_backend_licode_publish_erizo
                                        where dt='$do_date'
                                        group by split(connid,'_')[1],streamid, \`timestamp\`,connid
                                    )tt
                                        inner join 
                                    (
                                        select
                                            split(connid,'_')[1] conn_id,streamid,\`timestamp\` sub_ts,connid sub_long_connid
                                        from ${APP}.dwd_backend_licode_subscribe_start
                                        where dt='$do_date'
                                        group by split(connid,'_')[1],streamid, \`timestamp\`,connid
                                    )tt1
                                    on  tt.streamid=tt1.streamid and tt.conn_id=tt1.conn_id
                                )t2
                                on t1.conn_id=t2.conn_id
                            )t3
                            left join
                            (
                                select
                                    stream_id su_stream_id
                                from ${APP}.dwd_sdk_su
                                where dt='$do_date'
                                and conn_type='0'
                                and success_num=over_num
                                and ice_state in ('2','3')
                                group by stream_id
                            )t4
                            on t3.streamid=t4.su_stream_id
                        )t5
                        left join
                        (
                             select
                                stream_id du_stream_id
                            from ${APP}.dwd_sdk_du
                            where dt='$do_date'
                            and conn_type='0'
                            and success_num=over_num
                            and ice_state in ('2','3')
                            group by stream_id
                         )t6
                        on t5.streamid=t6.du_stream_id
                    )t7
                    left join
                    (
                       select
                            stream_id,\`timestamp\` unsub_ts ,conn_id unsub_connid
                       from ${APP}.dwd_backend_licode_unsubscribe
                        where dt='$do_date'
                        group by stream_id,\`timestamp\`,conn_id
                    )t8
                    on t7.streamid=t8.stream_id and sub_long_connid=t8.unsub_connid
                )t9
                left join
                (
                    select
                        stream_id unpub_stream_id,\`timestamp\` unpub_ts,conn_id unpub_connid
                   from ${APP}.dwd_backend_licode_unpublish
                    where dt='$do_date'
                    group by stream_id,\`timestamp\`,conn_id
                )t10
                on t9.streamid=t10.unpub_stream_id AND t9.pub_long_connid=t10.unpub_connid
            )t11
            left join
            (
                select
                    conn_id su_reason_conn_id,reason su_reason,\`timestamp\`
                from ${APP}.dwd_backend_licode_socket_disconnected
                where dt='$do_date'
                and type='viewer'
                group by conn_id,reason,\`timestamp\`
            )t12
            on t11.conn_id=t12.su_reason_conn_id
        )t13
        left join
        (
            select
                conn_id du_reason_conn_id,reason du_reason,\`timestamp\`
            from ${APP}.dwd_backend_licode_socket_disconnected
            where dt='$do_date'
            and type='presenter'
            group by conn_id,reason,\`timestamp\`
        )t14
        on t13.conn_id =t14.du_reason_conn_id
    )t15
    left join
    (
        select
            stream_id,conn_id,
            case
                when reason='0' then 'kNormal'
                when reason='1' then 'kToP2P'
                when reason='2' then 'kToRelay'
                when reason='3' then 'kToSFU'
                when reason='4' then 'kIceDisconnect'
                when reason='5' then 'kResetEncodeConfig'
                when reason='6' then 'kCleanResidualStream'
            else 'other' end unpub_reason
        from ${APP}.dwd_backend_licode_sdk_report
        where dt='$do_date'
        and type='presenter'
        group by stream_id,conn_id, case
                when reason='0' then 'kNormal'
                when reason='1' then 'kToP2P'
                when reason='2' then 'kToRelay'
                when reason='3' then 'kToSFU'
                when reason='4' then 'kIceDisconnect'
                when reason='5' then 'kResetEncodeConfig'
                when reason='6' then 'kCleanResidualStream'
            else 'other' end
    )t16
    on t15.streamid=t16.stream_id and t15.pub_long_connid=t16.conn_id
)t17;"

dws_banversion_total="
insert overwrite table ${APP}.dws_banversion_total partition (dt='$do_date')
select
    type,client,version,count(1) c1
from
(
    select
        type,client,version,id
    from
    (
        select
            if(errcode='30001','1','0') type,  
            if(errcode='30001',du_client,su_client) client,
            if(errcode='30001',dversion,sversion) version,
            if(errcode='30001',duid,suid) id
        from ${APP}.dwd_backend_banversion
        where dt ='$do_date'
        and errcode in('30001','10012')
    )t1
    group by type,client,version,id
)t2
group by type,client,version
order by type,c1 desc;"

dws_banversion_detail_total="
insert overwrite table ${APP}.dws_banversion_detail_total partition (dt='$do_date')
select
    type,client,old_version,new_version,c1,cast(c1/sum(c1) over(partition by type,client,old_version) *100 as DECIMAL(16,2))
from
(
    select
        type,client,old_version,new_version,count(1) c1
    from
    (
        select
            type,client,version old_version,id
        from
        (
            select
                if(errcode='30001','1','0') type, 
                if(errcode='30001',du_client,su_client) client,
                if(errcode='30001',dversion,sversion) version,
                if(errcode='30001',duid,suid) id
            from ${APP}.dwd_backend_banversion
            where dt =date_add('$do_date',-1)
            and errcode in('30001','10012')
        )t1
        group by type,client,version,id
    )t2
    left join
    (
        select
            suid,max(version) new_version
        from ${APP}.dwd_client_connect
        where dt in ('$do_date',date_add('$do_date',-1))
        and version is not null
        group by suid
    )t3
    on t2.id=t3.suid
    group by type,client,old_version,new_version
)t4
order by type,client desc,old_version desc,new_version desc;"

dws_backend_ec_error_total_4500="
insert overwrite table ${APP}.dws_backend_ec_error_total_4500 partition (dt='$do_date')
select   
    conn_id,'loss data' flag
from ${APP}.dws_backend_licode_day_total_4500
where dt='$do_date'
and forward_authentication_success='1'
and ec_success <> '1'
and ea_success in ('1','5')
group by conn_id
union all 
select
    total_connid,
    case when subend_connid is not null then 'viewer_loss data'
         when subend_connid is null and disconnect_connid is not null  then concat_ws('_',type,reason)
    else 'other' end reason
from
(
    select
        t4.conn_id total_connid,t5.conn_id subend_connid
    from
    (
        select
            t1.conn_id,ec_handshake_success,ec_success
        from
        (
            select
                conn_id,ec_handshake_success,ec_success
            from ${APP}.dws_backend_licode_day_total_4500
            where dt='$do_date'
            and forward_authentication_success='1'
            and ec_success <> '1'
            group by conn_id,ec_handshake_success,ec_success
        )t1
        left join
        (
            select
                conn_id
            from ${APP}.dws_backend_licode_day_total_4500
            where dt='$do_date'
            and forward_authentication_success='1'
            and ec_success <> '1'
            and ea_success in ('1','5')
            group by conn_id
        )t2
        on t1.conn_id=t2.conn_id
        where t2.conn_id is null and ec_success='2' and ec_handshake_success='1'
    )t4
    left join
    (
        select
            split(conn_id,'_')[2] conn_id
        from ${APP}.dwd_backend_licode_subscribe_end
        where dt='$do_date'
        group by split(conn_id,'_')[2]
    )t5
    on t4.conn_id=t5.conn_id
)t3
left join
(
    select
        conn_id disconnect_connid,type,reason
    from ${APP}.dwd_backend_licode_socket_disconnected
    where dt='$do_date'
    and type='viewer'
    group by conn_id, type, reason
)t4
on t3.total_connid=t4.disconnect_connid
union all  
select
    total_connid,
    case
         when subscribe_end_id is not null then 'viewer_loss data'
         when subscribe_end_id is null and t6.connid is not null then 'viewer_connect failed'
         when subscribe_end_id is null and t6.connid is null then 'viewer_not launch'
    else 'viewer_no log' end flag
from
(
    select
        t3.conn_id total_connid,t4.conn_id subscribe_end_id
    from
    (
        select
            t1.conn_id,ec_handshake_success,ec_success
        from
        (
            select
                conn_id,ec_handshake_success,ec_success
            from ${APP}.dws_backend_licode_day_total_4500
            where dt='$do_date'
            and forward_authentication_success='1'
            and ec_success <> '1'
            group by conn_id,ec_handshake_success,ec_success
        )t1
        left join
        (
            select
                conn_id
            from ${APP}.dws_backend_licode_day_total_4500
            where dt='$do_date'
            and forward_authentication_success='1'
            and ec_success <> '1'
            and ea_success in ('1','5')
            group by conn_id
        )t2
        on t1.conn_id=t2.conn_id
        where t2.conn_id is null and ec_success='2' and ec_handshake_success<>'1'
    )t3
    left join
    (
        select
            split(conn_id,'_')[2] conn_id
        from ${APP}.dwd_backend_licode_subscribe_end
        where dt='$do_date'
        group by split(conn_id,'_')[2]
    )t4
    on t3.conn_id=t4.conn_id
)t5
left join
(
    select
        connid
    from ${APP}.dwd_licode_connect_rtc
    where dt='$do_date'
    and flag in ('1','3')
)t6
on t5.total_connid=t6.connid
union all 
select
    total_connid,
    case when pubend_connid is not null then 'presenter_loss data'
         when pubend_connid is null and disconnect_connid is not null  then concat_ws('_',type,reason)
    else 'other' end reason
from
(
    select
        t3.conn_id total_connid,type,reason,disconnect_connid
    from
    (
        select
            t1.conn_id,ec_handshake_success,ec_success
        from
        (
            select
                conn_id,ec_handshake_success,ec_success
            from ${APP}.dws_backend_licode_day_total_4500
            where dt='$do_date'
            and forward_authentication_success='1'
            and ec_success <> '1'
            group by conn_id,ec_handshake_success,ec_success
        )t1
        left join
        (
            select
                conn_id
            from ${APP}.dws_backend_licode_day_total_4500
            where dt='$do_date'
            and ec_success <> '1'
            and forward_authentication_success='1'
            and ea_success in ('1','5')
            group by conn_id
        )t2
        on t1.conn_id=t2.conn_id
        where t2.conn_id is null and ec_success='3' and ec_handshake_success='1'
    )t3
    left join
    (
        select
            conn_id disconnect_connid,type,reason
        from ${APP}.dwd_backend_licode_socket_disconnected
        where dt='$do_date'
        and type='presenter'
        group by conn_id, type, reason
    )t4
    on t3.conn_id=t4.disconnect_connid
)t5
left join
(
     select
        split(conn_id,'_')[2] pubend_connid
    from ${APP}.dwd_backend_licode_publish_end
    where dt='$do_date'
    group by split(conn_id,'_')[2]
)t6
on t5.total_connid=t6.pubend_connid
union all 
select
    conn_id,
    case
         when pubend_connid is not null then 'presenter_loss data'
         when pubend_connid is null and t6.connid is not null then 'presenter_connect failed'
         when pubend_connid is null and t6.connid is null then 'presenter_not launch'
    else 'presenter_no log' end
from
(
    select
        t3.conn_id,pubend_connid
    from
    (
        select
            t1.conn_id,ec_handshake_success,ec_success
        from
        (
            select
                conn_id,ec_handshake_success,ec_success
            from ${APP}.dws_backend_licode_day_total_4500
            where dt='$do_date'
            and forward_authentication_success='1'
            and ec_success <> '1'
            group by conn_id,ec_handshake_success,ec_success
        )t1
        left join
        (
            select
                conn_id
            from ${APP}.dws_backend_licode_day_total_4500
            where dt='$do_date'
            and ec_success <> '1'
            and forward_authentication_success='1'
            and ea_success in ('1','5')
            group by conn_id
        )t2
        on t1.conn_id=t2.conn_id
        where t2.conn_id is null and ec_success='3' and ec_handshake_success<>'1'
    )t3
    left join
    (
        select
            split(conn_id,'_')[2] pubend_connid
        from ${APP}.dwd_backend_licode_publish_end
        where dt='$do_date'
        group by split(conn_id,'_')[2]
    )t4
    on t3.conn_id=t4.pubend_connid
)t5
left join
(
    select
        connid
    from ${APP}.dwd_licode_connect_rtc
    where dt='$do_date'
    and flag in ('1','2')
)t6
on t5.conn_id=t6.connid
union all  
select  conn_id,concat_ws('-',su,du)
from (
    select
        conn_id,
        case
                 when subend_connid is not null then 'viewer_loss data'
                 when subend_connid is null and connect_rtc_sub_id is not null then 'viewer_connect failed'
                 when subend_connid is null and connect_rtc_pub_id is null then 'viewer_not launch'
            else 'viewer_no log' end su,
        case
                 when pubend_connid is not null then 'presenter_loss data'
                 when pubend_connid is null and connect_rtc_pub_id is not null then 'presenter_connect failed'
                 when pubend_connid is null and connect_rtc_pub_id is null then 'presenter_not launch'
        else 'presenter_no log' end du
    from (
            select
                conn_id,pubend_connid,subend_connid,connect_rtc_sub_id,connect_rtc_pub_id
            from
             (
                select
                    t7.conn_id,t7.pubend_connid,t7.subend_connid,t8.connect_rtc_sub_id
                from
                 (
                    select
                        conn_id,subend_connid,pubend_connid
                     from
                      (
                            select
                                 conn_id, subend_connid
                            from
                            (
                                select
                                    t1.conn_id,ec_handshake_success,ec_success
                                from
                                (
                                    select
                                        conn_id,ec_handshake_success,ec_success
                                    from ${APP}.dws_backend_licode_day_total_4500
                                    where dt='$do_date'
                                    and forward_authentication_success='1'
                                    and ec_success <> '1'
                                    group by conn_id,ec_handshake_success,ec_success
                                )t1
                                left join
                                (
                                    select
                                        conn_id
                                    from ${APP}.dws_backend_licode_day_total_4500
                                    where dt='$do_date'
                                    and ec_success <> '1'
                                    and forward_authentication_success='1'
                                    and ea_success in ('1','5')
                                    group by conn_id
                                )t2
                                on t1.conn_id=t2.conn_id
                                where t2.conn_id is null and ec_success='4' and ec_handshake_success='4'
                            )t3
                            left join
                            (
                                select
                                    split(conn_id,'_')[2] subend_connid
                                from ${APP}.dwd_backend_licode_subscribe_end
                                where dt='$do_date'
                                group by split(conn_id,'_')[2]
                            )t4
                            on t3.conn_id=t4.subend_connid
                       )t5
                       left join
                       (
                           select
                               split(conn_id,'_')[2] pubend_connid
                           from ${APP}.dwd_backend_licode_publish_end
                           where dt='$do_date'
                           group by split(conn_id,'_')[2]
                       )t6
                       on  t5.conn_id=t6.pubend_connid
                 )t7
                 left join
                 (
                    select
                        connid connect_rtc_sub_id
                    from ${APP}.dwd_licode_connect_rtc
                    where dt='$do_date'
                    and flag in ('1','3')
                 )t8
                 on t7.conn_id=t8.connect_rtc_sub_id
             )t9
             left join
             (
                select
                    connid connect_rtc_pub_id
                 from ${APP}.dwd_licode_connect_rtc
                 where dt='$do_date'
                 and flag in ('1','2')
             )t10
             on t9.conn_id=t10.connect_rtc_pub_id
         )t11
)tt1
union all  
select
    total_connid,concat_ws('-',su,du)
from
(
    select
        total_connid,
        case
             when subend_connid is not null then 'viewer_loss data'
             when subend_connid is null and connect_rtc_sub_id is not null then 'viewer_connect failed'
             when subend_connid is null and connect_rtc_sub_id is null then 'viewer_not launch'
        else 'viewer_no log' end su,
        case when pubend_connid is not null then 'presenter_loss data'
             when pubend_connid is null and disconnect_id is not null  then concat_ws('_',type,reason)
        else 'other' end du
    from
     (
        select
            total_connid, disconnect_id, type, reason, subend_connid,connect_rtc_sub_id
        from
             (
                select
                    total_connid, connect_rtc_sub_id, disconnect_id, type, reason
                from (
                         select conn_id total_connid,
                                connect_rtc_sub_id
                         from (
                              select t1.conn_id,
                                     ec_handshake_success,
                                     ec_success
                              from (
                                       select conn_id,
                                              ec_handshake_success,
                                              ec_success
                                       from ${APP}.dws_backend_licode_day_total_4500
                                       where dt = '$do_date'
                                         and forward_authentication_success = '1'
                                         and ec_success <> '1'
                                       group by conn_id, ec_handshake_success, ec_success
                                   ) t1
                                       left join
                                   (
                                       select conn_id
                                       from ${APP}.dws_backend_licode_day_total_4500
                                       where dt = '$do_date'
                                         and ec_success <> '1'
                                         and forward_authentication_success='1'
                                         and ea_success in ('1', '5')
                                       group by conn_id
                                   ) t2
                                   on t1.conn_id = t2.conn_id
                              where t2.conn_id is null
                                and ec_success = '4'
                                and ec_handshake_success = '2'
                          ) t3
                              left join
                          (
                              select
                                    connid connect_rtc_sub_id
                              from ${APP}.dwd_licode_connect_rtc
                              where dt='$do_date'
                              and flag in ('1','3')
                          ) t4
                          on t3.conn_id = t4.connect_rtc_sub_id
                    ) t5
                    left join
                    (
                        select
                            conn_id disconnect_id,type,reason
                        from ${APP}.dwd_backend_licode_socket_disconnected
                        where dt='$do_date'
                        and type='presenter'
                        group by conn_id, type, reason
                    )t6
                    on t5.total_connid=t6.disconnect_id
         )t9
        left join
         (
            select
                split(conn_id,'_')[2] subend_connid
            from ${APP}.dwd_backend_licode_subscribe_end
            where dt='$do_date'
            group by split(conn_id,'_')[2]
         )t10
        on t9.total_connid=t10.subend_connid
     )t11
    left join
    (
        select
            split(conn_id,'_')[2] pubend_connid
        from ${APP}.dwd_backend_licode_publish_end
        where dt='$do_date'
        group by split(conn_id,'_')[2]
    )t12
    on t11.total_connid=t12.pubend_connid
)t9
union all  
select
    total_connid,concat_ws('-',su,du)
from
     (
        select
            total_connid,
            case
                     when pubend_connid is not null then 'presenter_loss data'
                     when pubend_connid is null and connect_rtc_sub_id is not null then 'presenter_connect failed'
                     when pubend_connid is null and connect_rtc_sub_id is null then 'presenter_not launch'
            else 'presenter_no log' end du,
            case when subend_connid is not null then 'viewer_loss data'
                 when subend_connid is null and disconnect_connid is not null  then concat_ws('_',type,reason)
            else 'other' end su
        from
         (
            select
                total_connid, connect_rtc_sub_id, disconnect_connid, type, reason,subend_connid
            from
             (
                select
                    total_connid ,connect_rtc_sub_id, disconnect_connid, type, reason
                from
                (
                    select
                         total_connid, connect_rtc_sub_id
                    from
                    (
                        select
                            t1.conn_id total_connid,ec_handshake_success,ec_success
                        from
                        (
                            select
                                conn_id,ec_handshake_success,ec_success
                            from ${APP}.dws_backend_licode_day_total_4500
                            where dt='$do_date'
                            and forward_authentication_success='1'
                            and ec_success <> '1'
                            group by conn_id,ec_handshake_success,ec_success
                        )t1
                        left join
                        (
                            select
                                conn_id
                            from ${APP}.dws_backend_licode_day_total_4500
                            where dt='$do_date'
                            and ec_success <> '1'
                            and forward_authentication_success='1'
                            and ea_success in ('1','5')
                            group by conn_id
                        )t2
                        on t1.conn_id=t2.conn_id
                        where t2.conn_id is null and ec_success='4' and ec_handshake_success='3'
                    )t3
                    left join
                    (
                        select
                            connid connect_rtc_sub_id
                        from ${APP}.dwd_licode_connect_rtc
                        where dt='$do_date'
                        and flag in ('1','2')
                    )t4
                    on  t3.total_connid=t4.connect_rtc_sub_id
                )t5
                left join
                (
                    select
                        conn_id disconnect_connid,type,reason
                    from ${APP}.dwd_backend_licode_socket_disconnected
                    where dt='$do_date'
                    and type='viewer'
                    group by conn_id, type, reason
                )t8
                on t5.total_connid=t8.disconnect_connid
             )t9
             left join
            (
                select
                    split(conn_id,'_')[2] subend_connid
                from ${APP}.dwd_backend_licode_subscribe_end
                where dt='$do_date'
                group by split(conn_id,'_')[2]
            )t10
            on t9.total_connid=t10.subend_connid
         )t11
        left join
         (
             select
                split(conn_id,'_')[2] pubend_connid
            from ${APP}.dwd_backend_licode_publish_end
            where dt='$do_date'
            group by split(conn_id,'_')[2]
         )t12
        on t11.total_connid=t12.pubend_connid
)t9
union all  
select
    id,concat_ws('-',su,du)
from
(
    select
        id,
        case when subend_connid is not null then 'viewer_loss data'
             when subend_connid is null and su_id is not null  then concat_ws('_',su_type,su_reason)
        else 'other' end su,
        case when pubend_connid is not null then 'presenter_loss data'
             when pubend_connid is null and du_id is not null  then concat_ws('_',du_type,du_reason)
        else 'other' end du
    from
     (
        select
            t7.id, su_id, su_type, su_reason, du_id, du_type, du_reason,t8.subend_connid
        from
        (
            select
                t5.id, su_id, su_type, su_reason,t6.du_id, du_type, du_reason
            from
            (
                select
                    t3.conn_id id, t4.conn_id su_id, su_type, su_reason
                from
                (
                    select
                        t1.conn_id
                    from
                    (
                        select
                            conn_id,ec_handshake_success,ec_success
                        from ${APP}.dws_backend_licode_day_total_4500
                        where dt='$do_date'
                        and forward_authentication_success='1'
                        and ec_success <> '1'
                        group by conn_id,ec_handshake_success,ec_success
                    )t1
                    left join
                    (
                        select
                            conn_id
                        from ${APP}.dws_backend_licode_day_total_4500
                        where dt='$do_date'
                        and ec_success <> '1'
                        and forward_authentication_success='1'
                        and ea_success in ('1','5')
                        group by conn_id
                    )t2
                    on t1.conn_id=t2.conn_id
                    where t2.conn_id is null and ec_success='4' and ec_handshake_success='1'
                )t3
                left join
                (
                    select
                        conn_id,type su_type,reason su_reason
                    from ${APP}.dwd_backend_licode_socket_disconnected
                    where dt='$do_date'
                    and type='viewer'
                    group by conn_id, type, reason
                )t4
                on  t3.conn_id=t4.conn_id
            )t5
            left join
            (
                select
                    conn_id du_id,type du_type,reason du_reason
                from ${APP}.dwd_backend_licode_socket_disconnected
                where dt='$do_date'
                and type='presenter'
                group by conn_id, type, reason
            )t6
            on t5.id=t6.du_id
        )t7
        left join
        (
            select
                split(conn_id,'_')[2] subend_connid
            from ${APP}.dwd_backend_licode_subscribe_end
            where dt='$do_date'
            group by split(conn_id,'_')[2]
        )t8
        on t7.id=t8.subend_connid
     )t9
     left join
    (
        select
            split(conn_id,'_')[2] pubend_connid
        from ${APP}.dwd_backend_licode_publish_end
        where dt='$do_date'
        group by split(conn_id,'_')[2]
    )t10
    on t9.id=t10.pubend_connid
)t14;"

dws_backend_ea_failed_total_4500="
insert overwrite table ${APP}.dws_backend_ea_failed_total_4500 partition (dt='$do_date')
select  
    t7.conn_id,t7.stream_id,
    case
        when su_stream_id is not null then 'loss data'
        when su_stream_id is null and unpub_str is not null then unpub_reason
        when su_stream_id is null and unpub_str is null and  rtc_id is not null then rtc_flag
        when su_stream_id is null and unpub_str is null and  rtc_id is null and dis_conid is not null then reason
    else 'other' end flag1
from
(
    select
        t5.stream_id,conn_id,su_stream_id,unpub_str,unpub_reason,rtc_id,rtc_flag
    from
    (
        select
            t3.stream_id,conn_id,su_stream_id,unpub_str,unpub_reason
        from
        (
            select
                t1.stream_id,conn_id,su_stream_id
            from
            (
                select
                    stream_id,conn_id
                from ${APP}.dws_backend_licode_ea_total_4500
                where dt='$do_date'
                and ea_success ='2'
                group by stream_id,conn_id
            )t1
            left join
            (
                select
                    stream_id su_stream_id
                from ${APP}.dwd_sdk_su
                where dt='$do_date'
                and conn_type='0'
                and success_num=over_num
                and ice_state in ('2','3')
                group by stream_id
            )t2
            on t1.stream_id=t2.su_stream_id
        )t3
        left join
        (
           select
            stream_id unpub_str,case
                when res='0' then 'kNormal'
                when res='1' then 'kToP2P'
                when res='2' then 'kToRelay'
                when res='3' then 'kToSFU'
                when res='4' then 'kIceDisconnect'
                when res='5' then 'kResetEncodeConfig'
                when res='6' then 'kCleanResidualStream'
            else 'other' end unpub_reason
        from
         (
            select
                stream_id,first_value(reason) over(partition by stream_id order by reason) res
            from
             (
                select
                    stream_id,reason
                from ${APP}.dwd_backend_licode_sdk_report
                where dt='$do_date'
                and type='presenter'
                group by stream_id,reason
             )t1
         )t2
        group by stream_id,res
        )t4
        on t3.stream_id=t4.unpub_str
    )t5
    left join
    (
       select
            conn_id rtc_id ,rtc_flag
        from
         (
             select
                conn_id,first_value(err) over ( partition by conn_id order by err desc) rtc_flag
             from
              (
                select
                    conn_id,case
                                when events = 'kill_RTC' then 'killRTC'
                                when events = 'close_RTC' then 'closeRTC'
                            end err
               from ${APP}.dwd_client_tuevent
                where dt='$do_date'
                and clientype='client'
                and events in ('kill_RTC','close_RTC')
                group by conn_id,case
                                when events = 'kill_RTC' then 'killRTC'
                                when events = 'close_RTC' then 'closeRTC' end
              )t0
         )t2
        group by conn_id,rtc_flag
    )t6
    on t5.conn_id=t6.rtc_id
)t7
left join
(
    select 
        conn_id dis_conid,first_value(reason) over(partition by conn_id order by \`timestamp\`) reason
    from ${APP}.dwd_backend_licode_socket_disconnected
    where dt='$do_date'
    and type='viewer'
)t8
on t7.stream_id=t8.dis_conid
union all 
select
    t7.conn_id,t7.stream_id,
    case
         when du_stream_id is not null then 'loss data'
         when du_stream_id is null and sdk_str is not null then unpub_reason
         when du_stream_id is null and sdk_str is null and  rtc_id is not null then rtc_flag
         when du_stream_id is null and sdk_str is null and  rtc_id is null and disconnect_id is not null then reason
    else 'other' end flag1
from
(
    select
        t5.stream_id,conn_id,du_stream_id,sdk_str,unpub_reason,rtc_id,rtc_flag
    from
    (
        select
            t3.stream_id,conn_id,du_stream_id,sdk_str,unpub_reason
            
        from
        (
            select
                t1.stream_id,conn_id,du_stream_id
            from
            (
                select
                    stream_id,conn_id
                from ${APP}.dws_backend_licode_ea_total_4500
                where dt='$do_date'
                and ea_success ='3'
                group by stream_id,conn_id
            )t1
            left join
            (
                select
                    stream_id du_stream_id
                from ${APP}.dwd_sdk_du
                where dt='$do_date'
                and conn_type='0'
                and success_num=over_num
                and ice_state in ('2','3')
                group by stream_id
            )t2
            on t1.stream_id=t2.du_stream_id
        )t3
        left join
        (
           select
                stream_id sdk_str,case
                    when res='0' then 'kNormal'
                    when res='1' then 'kToP2P'
                    when res='2' then 'kToRelay'
                    when res='3' then 'kToSFU'
                    when res='4' then 'kIceDisconnect'
                    when res='5' then 'kResetEncodeConfig'
                    when res='6' then 'kCleanResidualStream'
                else 'other' end unpub_reason
            from
             (
                select
                    stream_id,first_value(reason) over(partition by stream_id order by reason) res
                from
                 (
                    select
                        stream_id,reason
                    from ${APP}.dwd_backend_licode_sdk_report
                    where dt='$do_date'
                    and type='presenter'
                    group by stream_id,reason
                 )t1
             )t2
            group by stream_id,res
        )t4
        on t3.stream_id=t4.sdk_str
    )t5
    left join
    (
        select
            conn_id rtc_id ,rtc_flag
        from
         (
             select
                conn_id,first_value(err) over ( partition by conn_id order by err desc) rtc_flag
             from
              (
                select
                    conn_id,case
                                when errno = 'kill_RTC' then 'killRTC'
                                when errno = 'close_RTC' then 'closeRTC'
                            end err
               from ${APP}.dwd_client_tevent
                where dt='$do_date'
                and clientype='1'
                and errno in ('kill_RTC','close_RTC')
                group by conn_id,case
                                when errno = 'kill_RTC' then 'killRTC'
                                when errno = 'close_RTC' then 'closeRTC' end
              )t0
         )t2
        group by conn_id,rtc_flag
    )t6
    on t5.conn_id=t6.rtc_id
)t7
left join
(
    select 
        conn_id disconnect_id,first_value(reason) over(partition by conn_id order by \`timestamp\`) reason
    from ${APP}.dwd_backend_licode_socket_disconnected
    where dt='$do_date'
    and type='presenter'
)t8
on t7.conn_id=t8.disconnect_id
union all 
select
    conn_id,stream_id,concat_ws('-',su_flag,du_flag) flag
from
(
    select
        conn_id,stream_id,
        case
            when su_stream_id is not null then 'viewer_loss data'
            when su_stream_id is null and sdk_str is not null then unpub_reason
            when su_stream_id is null and sdk_str is null and  su_rtc_id is not null then concat('viewer_',su_rtc_flag)
            when su_stream_id is null and sdk_str is null and  su_rtc_id is null and su_disconnected_id is not null then concat('viewer_',su_disconnected_reason)
        else 'other' end su_flag,
        case
            when du_stream_id is not null then 'presenter_loss data'
            when du_stream_id is null and sdk_str is not null then unpub_reason
            when du_stream_id is null and sdk_str is null and  du_rtc_id is not null then concat('presenter_',du_rtc_flag)
            when du_stream_id is null and sdk_str is null and  du_rtc_id is null and du_disconnected_id is not null then concat('presenter_',du_disconnected_reason)
        else 'other' end du_flag
    from
    (
        select
            conn_id,stream_id,su_stream_id,du_stream_id,sdk_str,unpub_reason,su_rtc_id,su_rtc_flag,du_rtc_id,du_rtc_flag,su_disconnected_id,su_disconnected_reason
        from
        (
            select
                conn_id,stream_id,su_stream_id,du_stream_id,sdk_str,unpub_reason,su_rtc_id,su_rtc_flag,du_rtc_id,du_rtc_flag
            from
            (
                select
                    conn_id,stream_id,su_stream_id,du_stream_id,sdk_str,unpub_reason,su_rtc_id,su_rtc_flag
                from
                (
                    select
                        conn_id,stream_id,su_stream_id,du_stream_id,sdk_str,unpub_reason
                    from
                    (
                        select
                            conn_id,stream_id,su_stream_id,du_stream_id
                        from
                        (
                            select
                                t1.conn_id,stream_id,su_stream_id
                            from
                            (
                                select
                                    conn_id,stream_id
                                from ${APP}.dws_backend_licode_ea_total_4500
                                where dt='$do_date'
                                and ea_success ='4'
                                group by conn_id,stream_id
                            )t1
                            left join
                            (
                                select
                                    stream_id su_stream_id
                                from ${APP}.dwd_sdk_su
                                where dt='$do_date'
                                and conn_type='0'
                                and success_num=over_num
                                and ice_state in ('2','3')
                                group by stream_id
                            )t2
                            on t1.stream_id=t2.su_stream_id
                        )t3
                        left join
                        (
                             select
                                stream_id du_stream_id
                            from ${APP}.dwd_sdk_du
                            where dt='$do_date'
                            and conn_type='0'
                            and success_num=over_num
                            and ice_state in ('2','3')
                            group by stream_id
                         )t4
                        on t3.stream_id=t4.du_stream_id
                    )t5
                    left join
                    (
                        select
                            stream_id sdk_str,case
                                when res='0' then 'kNormal'
                                when res='1' then 'kToP2P'
                                when res='2' then 'kToRelay'
                                when res='3' then 'kToSFU'
                                when res='4' then 'kIceDisconnect'
                                when res='5' then 'kResetEncodeConfig'
                                when res='6' then 'kCleanResidualStream'
                            else 'other' end unpub_reason
                        from
                         (
                            select
                                stream_id,first_value(reason) over(partition by stream_id order by reason) res
                            from
                             (
                                select
                                    stream_id,reason
                                from ${APP}.dwd_backend_licode_sdk_report
                                where dt='$do_date'
                                and type='presenter'
                                group by stream_id,reason
                             )t1
                         )t2
                        group by stream_id,res
                    )t6
                    on t5.stream_id=t6.sdk_str
                )t7
                left join
                (
                    select
                        conn_id su_rtc_id ,su_rtc_flag
                    from
                     (
                         select
                            conn_id,first_value(err) over ( partition by conn_id order by err desc) su_rtc_flag
                         from
                          (
                            select
                                conn_id,case
                                            when events = 'kill_RTC' then 'killRTC'
                                            when events = 'close_RTC' then 'closeRTC'
                                        end err
                           from ${APP}.dwd_client_tuevent
                            where dt='$do_date'
                            and clientype='client'
                            and events in ('kill_RTC','close_RTC')
                            group by conn_id,case
                                            when events = 'kill_RTC' then 'killRTC'
                                            when events = 'close_RTC' then 'closeRTC' end
                          )t0
                     )t2
                    group by conn_id,su_rtc_flag
                )t8
                on t7.conn_id=t8.su_rtc_id
            )t9
            left join
            (
                select
                    conn_id du_rtc_id ,du_rtc_flag
                from
                 (
                     select
                        conn_id,first_value(err) over ( partition by conn_id order by err desc) du_rtc_flag
                     from
                      (
                        select
                            conn_id,case
                                        when errno = 'kill_RTC' then 'killRTC'
                                        when errno = 'close_RTC' then 'closeRTC'
                                    end err
                       from ${APP}.dwd_client_tevent
                        where dt='$do_date'
                        and clientype='1'
                        and errno in ('kill_RTC','close_RTC')
                        group by conn_id,case
                                        when errno = 'kill_RTC' then 'killRTC'
                                        when errno = 'close_RTC' then 'closeRTC' end
                      )t0
                 )t2
                group by conn_id,du_rtc_flag
            )t10
            on t9.conn_id=t10.du_rtc_id
        )t11
        left join
        (
            select 
                conn_id su_disconnected_id,first_value(reason) over(partition by conn_id order by \`timestamp\`) su_disconnected_reason
            from ${APP}.dwd_backend_licode_socket_disconnected
            where dt='$do_date'
            and type='viewer'
        )t12
        on t11.conn_id =t12.su_disconnected_id
    )t13
    left join
    (
        select 
            conn_id du_disconnected_id,first_value(reason) over(partition by conn_id order by \`timestamp\`) du_disconnected_reason
        from ${APP}.dwd_backend_licode_socket_disconnected
        where dt='$do_date'
        and type='presenter'
    )t14
    on t13.conn_id=t14.du_disconnected_id
)t15;"

dws_backend_licode_ea_total_4500="
insert overwrite table ${APP}.dws_backend_licode_ea_total_4500 partition (dt='$do_date')
select
    connid,streamid,if(str is null , '4',ea_err)
from(
    select
        t2.connid, streamid
    from
    (
        select
            connid
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        and sclient='PC'
        and dclient='PC'
        and action='2'
        and sversion >='4.5.0.0'
        and dversion >='4.5.0.0'
    )t1
    inner join
    (
        select
            t1.connid,t1.streamid
        from
         (
            select
                split(connid,'_')[2] connid,streamid
            from ${APP}.dwd_backend_licode_publish_erizo
            where dt='$do_date'
            group by connid, streamid
         )t1
         inner join
        (
            select
                streamid
            from ${APP}.dwd_backend_licode_subscribe_start
            where dt='$do_date'
            group by streamid
        )t2
        on t1.streamid=t2.streamid
    )t2
    on t1.connid=t2.connid
)t3
left join
(
    select
        nvl(t1.conn_id,t2.conn_id) id,
        nvl(t1.sub_str,t2.pub_str) str,
        case
            when t1.sub_str is not null and t2.pub_str is not null then '1'
            when t1.sub_str is null and t2.pub_str is not null then '2'
            when t1.sub_str is not null and t2.pub_str is null then '3'
    end ea_err
    from
    (
        select
            streamid sub_str,
            split(connid,'_')[2] conn_id
        from ${APP}.dwd_backend_licode_addsubscriber_ready
        where dt ='$do_date'
        and streamid is not null
        group by streamid, split(connid,'_')[2]
    )t1
    full outer join
    (
        select
            streamid pub_str,split(connid,'_')[2] conn_id
        from ${APP}.dwd_backend_licode_addPublisher_ready
        where dt ='$do_date'
        and streamid is not null
        group by streamid,split(connid,'_')[2]
    )t2
    on t1.sub_str=t2.pub_str
)t4
on  t3.streamid=t4.str;"

dws_mac_info="
set hive.merge.smallfiles.avgsize=80000000;
set hive.merge.size.per.task=192000000;
insert overwrite table ${APP}.dws_mac_info partition (dt='$do_date')
select
    id,mac_id, client, create_time,
    if(t1.first_su_time =0 and t2.first_su_time is not null,t2.first_su_time,t1.first_su_time),
    if(t2.last_su_time is not null,t2.last_su_time,t1.last_su_time),
    t1.su_cnt+if(t2.su_cnt is null,0,t2.su_cnt),
    if(t1.first_du_time =0 and t3.first_du_time is not null,t3.first_du_time,t1.first_du_time),
    if(t3.last_du_time is not null,t3.last_du_time,t1.last_du_time),
    t1.du_cnt+if(t3.du_cnt is null,0,t3.du_cnt),
    if(t1.first_connect_time =0 and t4.first_connect_time is not null,t4.first_connect_time,t1.first_connect_time),
    if(t4.last_connect_time is not null,t4.last_connect_time,t1.last_connect_time),
    t1.connect_cnt+if(t4.connect_cnt is null,0,t4.connect_cnt)
from
(
    select
        id, mac_id, client, create_time, first_su_time, last_su_time, su_cnt, first_du_time, last_du_time, du_cnt, first_connect_time, last_connect_time, connect_cnt
    from ${APP}.dws_mac_info
    where dt=date_sub('$do_date',1)
    union all
    select
        id,mac_id,cast(client_type as int),create_time,0,0,0,0,0,0,0,0,0
    from ${APP}.dim_mac_info
    where dt='$do_date'
    and create_time >= unix_timestamp('$do_date 00:00:00') - 28800
    and create_time <  unix_timestamp('$do_date 00:00:00') + 57600
)t1
left join
(
    select
        suid,min(ts) first_su_time,max(ts) last_su_time,count(1) su_cnt
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    group by suid
)t2
on t1.mac_id=t2.suid
left join
(
    select
        duid,min(ts) first_du_time,max(ts) last_du_time,count(1) du_cnt
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    group by duid
)t3
on t1.mac_id=t3.duid
left join
(
     select
        suid,min(ts) first_connect_time,max(ts) last_connect_time,count(1) connect_cnt
    from ${APP}.dwd_client_connect
    where dt='$do_date'
    group by suid
)t4
on t1.mac_id=t4.suid;"

dws_mac_connect_active="
insert overwrite table ${APP}.dws_mac_connect_active partition(dt='$do_date')
select
    suid,max(client) pl
from ${APP}.dwd_client_connect
where dt ='$do_date'
and client in ('PC', 'MAC', 'IOS', 'ANDROID','ONTHER','TV','LINUX')
and labtype !='daas'
and enterprise='0'
group by suid;"

case $1 in
    "dws_serv_total" )
	hive -e "$dws_serv_total"
    ;;
    "dws_presenter_lost_total" )
        hive -e "$dws_presenter_lost_total"
    ;;
    "dws_bus_user_action" )
        hive -e "$dws_bus_user_action"
    ;;
    "dws_user_action" )
        hive -e "$dws_user_action"
    ;;
    "dws_mac_action" )
        hive -e "$dws_mac_action"
    ;;
    "dws_su_action" )
        hive -e "$dws_su_action"
    ;;
    "dws_du_action" )
        hive -e "$dws_du_action"
    ;;
    "dws_hour_total" )
	hive -e "$dws_hour_total"
    ;;
    "dws_serv_connection_failure" )
	hive -e "$dws_serv_connection_failure"
    ;;
    "dws_su_action_daytotal" )
	hive -e "$dws_su_action_daytotal"
    ;;
    "dws_du_action_daytotal" )
	hive -e "$dws_du_action_daytotal"
    ;;
    "dws_client_daytotal" )
	hive -e "$dws_client_daytotal"
    ;;
    "dws_province_customer_day_total" )
	hive -e "$dws_province_customer_day_total"
    ;;
    "dws_client_version_licode_day_total" )
	hive -e "$dws_client_version_licode_day_total"
    ;;
    "dws_client_session_day" )
	hive -e "$dws_client_session_day"
    ;;
    "dws_province_isp_client_day_total" )
	hive -e "$dws_province_isp_client_day_total"
    ;;
    "dws_ip_licode_day_total" )
	hive -e "$dws_ip_licode_day_total"
    ;;
    "dws_country_vip_total" )
	hive -e "$dws_country_vip_total"
    ;;
    "dws_backend_licode_day_total" )
	hive -e "$dws_backend_licode_day_total"
    ;;
    "dws_backend_ec_error_total" )
	hive -e "$dws_backend_ec_error_total"
    ;;
    "dws_backend_ea_error_total" )
	hive -e "$dws_backend_ea_error_total"
    ;;
    "dws_banversion_total" )
	hive -e "$dws_banversion_total"
    ;;
    "dws_banversion_detail_total" )
	hive -e "$dws_banversion_detail_total"
    ;;
    "dws_backend_licode_day_total_4500" )
	hive -e "$dws_backend_licode_day_total_4500"
    ;;
    "dws_backend_ec_error_total_4500" )
	hive -e "$dws_backend_ec_error_total_4500"
    ;;
    "dws_backend_ea_error_total_4500" )
	hive -e "$dws_backend_ea_error_total_4500"
    ;;
    "dws_backend_ea_failed_total_4500" )
	hive -e "$dws_backend_ea_failed_total_4500"
    ;;
    "dws_backend_licode_ea_total_4500" )
	hive -e "$dws_backend_licode_ea_total_4500"
    ;;
    "dws_mac_info" )
	hive -e "$dws_mac_info"
    ;;
    "dws_mac_connect_active" )
	hive -e "$dws_mac_connect_active"
    ;;
    "ea" )
	hive -e "$dws_backend_licode_day_total_4500$dws_backend_ec_error_total_4500$dws_backend_ea_error_total_4500$dws_backend_licode_ea_total_4500$dws_backend_ea_failed_total_4500"
    ;;
    "cc" )
        hive -e "$dws_serv_total$dws_hour_total$dws_presenter_lost_total$dws_bus_user_action$dws_user_action$dws_mac_action$dws_su_action$dws_du_action$dws_serv_connection_failure$dws_su_action_daytotal$dws_du_action_daytotal$dws_province_customer_day_total"
    ;;
    "cc1" )
	hive -e "$dws_client_version_licode_day_total$dws_client_session_day$dws_province_isp_client_day_total$dws_ip_licode_day_total$dws_country_vip_total$dws_banversion_total$dws_banversion_detail_total$dws_mac_info$dws_mac_connect_active"
    ;;
    "all" )
        hive -e "$dws_serv_total$dws_hour_total$dws_presenter_lost_total$dws_bus_user_action$dws_user_action$dws_mac_action$dws_su_action$dws_du_action$dws_serv_connection_failure$dws_su_action_daytotal$dws_du_action_daytotal$dws_province_customer_day_total$dws_client_version_licode_day_total$dws_client_session_day$dws_province_isp_client_day_total$dws_ip_licode_day_total$dws_country_vip_total$dws_banversion_total$dws_banversion_detail_total$dws_mac_info$dws_mac_connect_active"
    ;;
esac

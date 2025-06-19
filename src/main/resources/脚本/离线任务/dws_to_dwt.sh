#!/bin/bash

APP=todesk
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d "-1 day" +%F`
fi

dwt_bus_user_topic="
insert overwrite table ${APP}.dwt_bus_user_topic partition(dt='$do_date')
select
    nvl(t2.id,t1.id),
    nvl(t2.bus,t1.bus),
    nvl(t2.action,t1.action),
    if(t1.id is null and t1.bus is null and t1.action is null,'$do_date',t1.first_login_date),
    if(t2.id is not null and t2.bus is not null and t2.action = 'start_front','$do_date',t1.last_start_front_date),
    if(t2.id is not null and t2.bus is not null and t2.action = 'start_process','$do_date',t1.last_start_process_date)
from
(
    select
        *
    from ${APP}.dwt_bus_user_topic
    where dt=date_add('$do_date',-1)
)t1
full outer join
(
    select
        *
    from ${APP}.dws_bus_user_action
    where dt='$do_date'
)t2
on t1.id=t2.id and t1.bus=t2.bus and t1.action=t2.action;"

dwt_user_topic="
insert overwrite table ${APP}.dwt_user_topic partition(dt='$do_date')
select
    nvl(t2.id,t1.id),
    nvl(t2.action_type,t1.action),
    if(t1.id is null and t1.action is null,'$do_date',t1.first_login_date),
    if(t2.id is not null and t2.action_type = 'start_front','$do_date',t1.last_start_front_date),
    if(t2.id is not null and t2.action_type = 'start_process','$do_date',t1.last_start_process_date)
from
(
    select
        *
    from ${APP}.dwt_user_topic
    where dt=date_add('$do_date',-1)
)t1
full outer join
(
    select
        *
    from ${APP}.dws_user_action
    where dt='$do_date'
)t2
on t1.id=t2.id and t1.action=t2.action_type;"

dwt_mac_topic="
insert overwrite table ${APP}.dwt_mac_topic partition(dt='$do_date')
select
    nvl(t2.id,t1.id),
    nvl(t2.client_type,t1.client_type),
    nvl(t2.action_type,t1.action_type),
    if(t1.id is null and t1.client_type is null and t1.action_type is null,'$do_date',t1.first_login_date),
    if(t2.id is not null and t2.client_type is not null and t2.action_type = 'start_front','$do_date',t1.last_start_front_date),
    if(t2.id is not null and t2.client_type is not null and t2.action_type = 'start_process','$do_date',t1.last_start_process_date)
from
(
    select
        *
    from ${APP}.dwt_mac_topic
    where dt=date_add('$do_date',-1)
)t1
full outer join
(
    select
        id,
        client_type,
        action_type
    from ${APP}.dws_mac_action
    where dt='$do_date'
)t2
on t1.id=t2.id and t1.client_type = t2.client_type and t1.action_type=t2.action_type;"

dwt_su_topic="
insert overwrite table ${APP}.dwt_su_topic partition(dt='$do_date')
select
    nvl(t2.id,t1.id),
    nvl(t2.client_type,t1.client_type),
    nvl(t2.action_type,t1.action_type),
    if(t1.id is null and t1.client_type is null and t1.action_type is null,'$do_date',t1.first_login_date),
    if(t2.id is not null and t2.client_type is not null and t2.action_type = 'start_front','$do_date',t1.last_start_front_date),
    if(t2.id is not null and t2.client_type is not null and t2.action_type = 'start_process','$do_date',t1.last_start_process_date)
from
(
    select
        *
    from ${APP}.dwt_su_topic
    where dt=date_add('$do_date',-1)
)t1
full outer join
(
    select
        id,
        client_type,
        action_type
    from ${APP}.dws_su_action
    where dt='$do_date'
    and client_type is not null
)t2
on t1.id=t2.id and t1.client_type=t2.client_type and t1.action_type=t2.action_type;"

dwt_du_topic="
insert overwrite table ${APP}.dwt_du_topic partition(dt='$do_date')
select
    nvl(t2.id,t1.id),
    nvl(t2.client_type,t1.client_type),
    nvl(t2.action_type,t1.action_type),
    if(t1.id is null and t1.client_type is null and t1.action_type is null,'$do_date',t1.first_login_date),
    if(t2.id is not null and t2.client_type is not null and t2.action_type = 'start_front','$do_date',t1.last_start_front_date), 
    if(t2.id is not null and t2.client_type is not null and t2.action_type = 'start_process','$do_date',t1.last_start_process_date)
from
(
    select
        *
    from ${APP}.dwt_du_topic
    where dt=date_add('$do_date',-1)
)t1
full outer join
(
    select
        id,
        client_type,
        action_type
    from ${APP}.dws_du_action
    where dt='$do_date'
    and client_type is not null
)t2
on t1.id=t2.id and t1.client_type=t2.client_type and t1.action_type=t2.action_type;"

dwt_su_total_topic="
insert overwrite table ${APP}.dwt_su_total_topic partition(dt='$do_date')
select
    nvl(t2.mac_id,t1.mac_id),
    nvl(t2.client_type,t1.client_type),
    if(t1.mac_id is null and t1.client_type is null ,'$do_date',t1.first_su_date),
    if(t2.mac_id is not null and t2.client_type is not null ,'$do_date',t1.last_su_date) 
from
(
    select
        *
    from ${APP}.dwt_su_total_topic
    where dt=date_add('$do_date',-1)
)t1
full outer join
(
    select
        mac_id,
        client_type
    from ${APP}.dws_su_action_daytotal 
    where dt='$do_date'
)t2
on t1.mac_id=t2.mac_id and t1.client_type=t2.client_type;"

dwt_du_total_topic="
insert overwrite table ${APP}.dwt_du_total_topic partition(dt='$do_date')
select
    nvl(t2.mac_id,t1.mac_id),
    nvl(t2.client_type,t1.client_type),
    if(t1.mac_id is null and t1.client_type is null ,'$do_date',t1.first_du_date), 
    if(t2.mac_id is not null and t2.client_type is not null ,'$do_date',t1.last_du_date) 
from
(
    select
        *
    from ${APP}.dwt_du_total_topic
    where dt=date_add('$do_date',-1)
)t1
full outer join
(
    select
        mac_id,
        client_type
    from ${APP}.dws_du_action_daytotal
    where dt='$do_date'
)t2
on t1.mac_id=t2.mac_id and t1.client_type=t2.client_type;"

dwt_client_topic="
insert overwrite table ${APP}.dwt_client_topic partition(dt='$do_date')
select
    nvl(t2.mac_id,t1.mac_id),
    nvl(t2.client_type,t1.client_type),
    if(t1.mac_id is null and t1.client_type is null ,'$do_date',t1.first_logic_date), 
    if(t2.mac_id is not null and t2.client_type is not null ,'$do_date',t1.last_logic_date) 
from
(
    select
        *
    from ${APP}.dwt_client_topic
    where dt=date_add('$do_date',-1)
)t1
full outer join
(
    select
        mac_id,
        client_type
    from ${APP}.dws_client_daytotal
    where dt='$do_date'
)t2
on t1.mac_id=t2.mac_id and t1.client_type=t2.client_type;"

dwt_android_topic="
insert overwrite table ${APP}.dwt_android_topic partition(dt='$do_date')
select
    id,client_type,if(bus is null ,'',bus),first,last
from
(
    select
        nvl(t2.id,t1.id) id,
        nvl(t2.platform,t1.client_type) client_type,
        nvl(t2.bus,t1.bus) bus,
        if(t1.id is null and t1.client_type is null ,'$do_date',t1.first_logic_date) first,
        if(t2.id is not null and t2.platform is not null ,'$do_date',t1.last_logic_date) last
    from
    (
        select
            id,client_type,bus,first_logic_date,last_logic_date
        from ${APP}.dwt_android_topic
        where dt=date_add('$do_date',-1)
    )t1
    full outer join
    (
        select 
            id, bus, platform
        from
         (
             select
                  
                  id,
                  bus,
                  platform,
                  logtime,
                  min(logtime) over (partition by id,platform) o1
              from ${APP}.dwd_client_tuevent
              where dt = '$do_date'
                and events = 'link_tech'
                and action = '15'
                and platform in ('android', 'iOS')
         )t0
        where logtime=o1
        group by id, bus, platform
    )t2
    on t1.id=t2.id and t1.client_type=t2.platform
)t3;"

dwt_su_new_total_topic="
insert overwrite table ${APP}.dwt_su_new_total_topic partition(dt='$do_date')
select
    nvl(t2.mac_id,t1.mac_id),
    nvl(t2.client_type,t1.client_type),
    
    if(t1.mac_id is null and t1.client_type is null and t2.mac_id is not null and t2.tdid is not null,'$do_date',t1.first_su_date), 
    if(t2.mac_id is not null and t2.client_type is not null ,'$do_date',t1.last_su_date) 
from
(
    select
        *
    from ${APP}.dwt_su_new_total_topic
    where dt=date_add('$do_date',-1)
)t1
full outer join
(
    select
        mac_id, client_type,tdid
    from
    (
        select
            mac_id,
            client_type
        from ${APP}.dws_su_action_daytotal
        where dt='$do_date'
    )t1
    full outer join
    (
        select
            tdid
        from ${APP}.dwd_new_user
        where dt='$do_date'
	and labtype='logic'
        group by tdid
    )tt
    on mac_id=tdid
)t2
on t1.mac_id=t2.mac_id and t1.client_type=t2.client_type;"

dwt_du_new_total_topic="
insert overwrite table ${APP}.dwt_du_new_total_topic partition(dt='$do_date')
select
    nvl(t2.mac_id,t1.mac_id),
    nvl(t2.client_type,t1.client_type),
    
    if(t1.mac_id is null and t1.client_type is null and t2.mac_id is not null and t2.tdid is not null ,'$do_date',t1.first_du_date), 
    if(t2.mac_id is not null and t2.client_type is not null ,'$do_date',t1.last_du_date) 
from
(
    select
        *
    from ${APP}.dwt_du_new_total_topic
    where dt=date_add('$do_date',-1)
)t1
full outer join
(
    select
        mac_id, client_type,tdid
    from
    (
        select
            mac_id,
            client_type
        from ${APP}.dws_du_action_daytotal
        where dt='$do_date'
    )t1
    full outer join
    (
        select
            tdid
        from ${APP}.dwd_new_user
        where dt='$do_date'
        and labtype='logic'
        group by tdid
    )tt
    on mac_id=tdid
)t2
on t1.mac_id=t2.mac_id and t1.client_type=t2.client_type;"

case $1 in
     "dwt_bus_user_topic" )
         hive -e "$dwt_bus_user_topic"
     ;;
     "dwt_user_topic" )
         hive -e "$dwt_user_topic"
     ;;
     "dwt_mac_topic" )
         hive -e "$dwt_mac_topic"
     ;;
     "dwt_su_topic" )
         hive -e "$dwt_su_topic"
     ;;
     "dwt_du_topic" )
         hive -e "$dwt_du_topic"
     ;;
     "dwt_su_total_topic" )
	 hive -e "$dwt_su_total_topic"
     ;;
     "dwt_du_total_topic" )
	 hive -e "$dwt_du_total_topic"
     ;;
     "dwt_client_topic" )
	hive -e "$dwt_client_topic"
     ;;
     "dwt_android_topic" )
	hive -e "$dwt_android_topic"
     ;;
     "dwt_su_new_total_topic" )
	hive -e "$dwt_su_new_total_topic"
     ;;
     "dwt_du_new_total_topic" )
	hive -e "$dwt_du_new_total_topic"
     ;;
     "tmp" )
	hive -e "$dwt_su_new_total_topic$dwt_du_new_total_topic"
     ;;
     "all" )
        hive -e "$dwt_bus_user_topic$dwt_user_topic$dwt_mac_topic$dwt_su_topic$dwt_du_topic"
    ;;
    "all_1" )
	hive -e "$dwt_su_total_topic$dwt_du_total_topic$dwt_android_topic$dwt_su_new_total_topic$dwt_du_new_total_topic"
    ;;
esac

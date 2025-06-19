#!/bin/bash

APP=todesk

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

ads_session_day="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_session_day
select
    '$do_date',
        enterprise
    ,reconnect,
    sum(if(handshake_success_connid is not null,1,0)) c1,
    sum(if(authentication_success_connid is not null,1,0)) c1,
    count(1) c2,
    cast(sum(if(handshake_success_connid is not null,1,0)) /count(t1.connid) * 100 as decimal(16,2)),
    cast(sum(if(authentication_success_connid is not null,1,0)) /count(t1.connid) * 100 as decimal(16,2))
from
(
    select connid,
           case
            when enterprise='0' and cast(regdays  as bigint) between 0 and 7 then 'consumer_new'
            when enterprise='0' and cast(regdays  as bigint) >= 7 then 'consumer_old'
            when enterprise='1' then 'business'
        else enterprise end enterprise,
	   case when reconnect is null and enterprise='1' then '0'
                 when reconnect is not null then reconnect
            else 'other' end reconnect
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and sprovince is not null
    and enterprise is not null
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
    select
           connid handshake_success_connid
    from ${APP}.dwd_handshake_success
    where dt='$do_date'
    group by connid
)t3
on t1.connid=t3.handshake_success_connid
group by t1.enterprise,reconnect;"

ads_process_session_day="
insert overwrite table ${APP}.ads_process_session_day
select
    '$do_date',
    labtype,reconnect,
    sum(if(handshake_success_connid is not null,1,0)) c1,
    sum(if(authentication_success_connid is not null,1,0)) c1,
    count(1) c2,
    cast(sum(if(handshake_success_connid is not null,1,0)) /count(t1.connid) * 100 as decimal(16,2)),
    cast(sum(if(authentication_success_connid is not null,1,0)) /count(t1.connid) * 100 as decimal(16,2))
from
(
    select connid,
           labtype,reconnect
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and reconnect is not null
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
group by labtype,reconnect;"

ads_su_client_version_total="
insert overwrite table ${APP}.ads_su_client_version_total
select
    '$do_date',
    sclient,
    sversion,
    count(1) c1
from ${APP}.dwd_new_connect
where dt='$do_date'
group by sclient,sversion;"

ads_du_client_version_total="
insert overwrite table ${APP}.ads_du_client_version_total
select
    '$do_date',
    dclient,
    dversion,
    count(1) c1
from ${APP}.dwd_new_connect
where dt='$do_date'
group by dclient,dversion;"

ads_conn_duration="
insert overwrite table ${APP}.ads_conn_duration
select
    '$do_date',
    case
        when t1.enterprise='0' then 'consumer'
        when t1.enterprise='1' then 'business'
    else t1.enterprise end enterprise,
    t1.c1,
    t1.c2,
    cast(t1.c1/t1.c2 as BIGINT)
from
(
    select
        enterprise, 
        sum(link_time) c1, 
        count(*) c2
    from
    ${APP}.dwd_session_success
    where dt='$do_date'
    and cast(link_time as bigint) > 0
    and enterprise is not null
    group by enterprise
)t1;"

ads_client_connect="
insert overwrite table ${APP}.ads_client_connect
select
       '$do_date',
       count(*)
from ${APP}.dwd_client_connect
where dt='$do_date';"

ads_client_closed="
insert overwrite table ${APP}.ads_client_closed
select
       '$do_date',
       count(*)
from ${APP}.dwd_client_closed
where dt='$do_date';"

ads_user_logout="
insert overwrite table ${APP}.ads_user_logout
select
       '$do_date',
       count(*)
from ${APP}.dwd_user_logout
where dt='$do_date';"

ads_add_device="
insert overwrite table ${APP}.ads_add_device
select
       '$do_date',
       count(*)
from ${APP}.dwd_add_device
where dt='$do_date';"

ads_delete_device="
insert overwrite table ${APP}.ads_delete_device
select
       '$do_date',
       count(*)
from ${APP}.dwd_delete_device
where dt='$do_date';"

ads_edit_device="
insert overwrite table ${APP}.ads_edit_device
select
       '$do_date',
       count(*)
from ${APP}.dwd_edit_device
where dt='$do_date';"

ads_add_group="
insert overwrite table ${APP}.ads_add_group
select
       '$do_date',
       count(*)
from ${APP}.dwd_add_group
where dt='$do_date';"

ads_delete_group="
insert overwrite table ${APP}.ads_delete_group
select
       '$do_date',
       count(*)
from ${APP}.dwd_delete_group
where dt='$do_date';"

ads_edit_group="
insert overwrite table ${APP}.ads_edit_group
select
       '$do_date',
       count(*)
from ${APP}.dwd_edit_group
where dt='$do_date';"

ads_android_logic_total="
insert overwrite table ${APP}.ads_android_logic_total
select
    '$do_date',
    count(*),
    sum(c1)
from
(
    select
        id,
        count(*) c1
    from ${APP}.dwd_client_tuevent
    where dt='$do_date'
    and events='link_tech'
    and action='15'
    group by id
)t1;"

ads_android_connect_total="
insert overwrite table ${APP}.ads_android_connect_total
select
    '$do_date',
    count(*),
    sum(c1)
from
(
    select count(*) c1
    from ${APP}.dwd_client_tuevent
    where dt='$do_date'
    and events='link_tech'
    and action='1'
    group by id
)t1;"

ads_phone_new_user="
insert overwrite table ${APP}.ads_phone_new_user
select
    '$do_date',
    case
       when platform = '3' then 'ios'
       when platform = '4' then 'android'
    end platform,
    count(distinct(tdid))
from ${APP}.dwd_new_user
where dt='$do_date'
and platform in('3','4')
and labtype='logic'
group by platform;"

ads_android_su_action="
insert overwrite table ${APP}.ads_android_su_action
select
    '$do_date',
    case
       when action=1 then 'connect_post'
       when action=7 then 'udx_post'
       when action=9 then 'upnp_post'
       when action=13 then 'su_disconnect'
    end action,
    count(*) c1
from ${APP}.dwd_client_tuevent
where dt='$do_date'
and platform = 'android'
and events='link_tech'
and (action='1' or action='7' or action='9' or action='13')
group by action
order by c1;"

ads_android_bus="
insert overwrite table ${APP}.ads_android_bus
select
    '$do_date',
    bus,
    count(distinct(id))
from ${APP}.dwd_client_tuevent
where dt='$do_date'
and platform='android'
and cast(id as BIGINT) > 0
and bus <> ''
group by bus;"

ads_android_resolution="
insert overwrite table ${APP}.ads_android_resolution
select
    '$do_date',
    resolution,
    count(distinct(id)) c1
from ${APP}.dwd_client_tuevent
where dt='$do_date'
and platform='android'
and cast(id as BIGINT) > 0
and bus <> ''
group by resolution
order by c1 desc
limit 100;"

ads_android_bios="
insert overwrite table ${APP}.ads_android_bios
select
    '$do_date',
    platform,
    vendor,
    bios,
    count(distinct(id)) c1
from ${APP}.dwd_client_tuevent
where dt='$do_date'
and platform in ('android','iOS')
and bios is not null
and bios <> ''
and vendor is not null
and cast(id as BIGINT) > 0
group by platform,bios,vendor
order by c1 desc
limit 100;"

ads_android_signin_total="
insert overwrite table ${APP}.ads_android_signin_total
select
    '$do_date',
    sum(if(ck_url='1' or get_json_object(ck_url,'$.result_code')='1',1,0)),
    count(1),
    cast(sum(if(ck_url='1' or get_json_object(ck_url,'$.result_code')='1',1,0)) / count(1) *100 as DECIMAL(16,2))
from ${APP}.dwd_client_click
where dt='$do_date'
and platform='android'
and module='account'
and action='signin'
and page='signin';"

ads_sclient_session="
insert overwrite table ${APP}.ads_sclient_session
select
    '$do_date',
    sclient,
    t4.c1,
    t4.c2,
    cast(t4.c1/t4.c2*100 as decimal(16,2))
from
(
    select
        sclient,
        sum(if(success_connid is not null,1,0)) c1,
        count(t3.connid) c2
    from(
        select
            *
        from
        (
            select connid,
                   sclient
            from ${APP}.dwd_new_connect
            where dt='$do_date'
        )t1
        left join
        (
            select
                   connid success_connid,
                   dt
            from ${APP}.dwd_authentication_success
            where dt='$do_date'
        )t2
        on t1.connid=t2.success_connid
    )t3
    group by t3.sclient
)t4
order by sclient;"

ads_phone_user_version="
insert overwrite table ${APP}.ads_phone_user_version
select
    '$do_date',
    t1.platform,
    t1.version,
    count(*)
from
(
    select
        platform,
        version,
        id,
        count(*)
    from ${APP}.dwd_client_tuevent
    where dt='$do_date'
    and (platform='android' or platform='iOS')
    group by platform,version,id
)t1
group by t1.platform, t1.version
order by platform ,version;"

ads_rtc_conn_total="
insert overwrite table ${APP}.ads_rtc_conn_total
select
    '$do_date',
    t3.type,
    t3.type_cnt,
    t3.total,
    cast(t3.type_cnt/t3.total*100 as DECIMAL(16,2))
from
(
    select
        t2.type,
        t2.type_cnt type_cnt,
        sum(type_cnt) over() total
    from
    (
        select
            t1.type type,
            count(*) type_cnt
        from
        (
            select
                case
                    when (local_type like  '%relay%' or remote_type like '%relay%') then 'relay'
                    else 'p2p' end as type
            from ${APP}.dwd_sdk_su
            where dt='$do_date'
        )t1
        group by t1.type
    )t2
)t3
order by type;"

ads_rtc_vedio_total="
insert overwrite table ${APP}.ads_rtc_vedio_total
select
    '$do_date',
    t1.type,
    sum(if(t1.first_frame_time >0,1,0)),
    count(*),
    cast(sum(if(t1.first_frame_time > 0,1,0)) / count(*) *100 as DECIMAL(16,2))
from
(
    select
        first_frame_time,
        case
            when (local_type like  '%relay%' or remote_type like '%relay%') then 'relay'
            else 'p2p' end as type
    from ${APP}.dwd_sdk_su
    where dt='$do_date'
    and conn_time > 0
)t1
group by type
order by type;"

ads_rtc_conn_fst_time="
insert overwrite table ${APP}.ads_rtc_conn_fst_time
select
    '$do_date',
    type,
    concat_ws(',',collect_set(cast(avgConnTime as STRING))),
    concat_ws(',',collect_set(cast(maxConnTime as STRING))),
    concat_ws(',',collect_set(cast(tmp1 as STRING))),
    concat_ws(',',collect_set(cast(avgFstTime as STRING))),
    concat_ws(',',collect_set(cast(maxFstTime as STRING))),
    concat_ws(',',collect_set(cast(tmp2 as STRING))),
    concat_ws(',',collect_set(cast(avgStuckRatio as STRING))),
    concat_ws(',',collect_set(cast(tmp_stuckRatio as STRING))),
    concat_ws(',',collect_set(cast(cnt as STRING)))
from
(
    select
        type,
        avgConnTime,
        maxConnTime,
        tmp1,
        avgFstTime,
        maxFstTime,
        tmp2,
        avgStuckRatio,
        case
            when co=1 then concat('0.1:',tmp3)
            when co=2 then concat('0.3:',tmp3)
            when co=3 then concat('0.5:',tmp3)
            when co=4 then concat('0.7:',tmp3)
            when co=5 then concat('0.9:',tmp3)
        end tmp_stuckRatio,
        cnt
    from
    (
        select
            type,
            avgConnTime,
            maxConnTime,
            cast(tmp1 as BIGINT) tmp1,
            avgFstTime,
            maxFstTime,
            cast(tmp2 as BIGINT) tmp2,
            avgStuckRatio,
            cast(tmp3*100 as DECIMAL(16,2)) tmp3,
            cnt,
            count(*) over(partition by type rows between UNBOUNDED PRECEDING AND CURRENT ROW ) co
        from
        (
            select
                type,
                cast(avg(conn_time) as BIGINT) as avgConnTime,
                max(cast(conn_time as BIGINT)) as maxConnTime,
                percentile_approx(cast(conn_time as BIGINT), array(0.1,0.3,0.5,0.7,0.9)) as  connTimePercentile,
                cast(avg(first_frame_time) as BIGINT) as avgFstTime,
                max(cast(first_frame_time as BIGINT)) as maxFstTime,
                percentile_approx(cast(first_frame_time as BIGINT), array(0.1,0.3,0.5,0.7,0.9)) as fstTimePercentile,
                cast(avg(dec_stuck_time/(1000*conn_time)*100) as DECIMAL(16,2)) avgStuckRatio,
                percentile_approx(dec_stuck_time/(1000*conn_time ) ,array(0.1,0.3,0.5,0.7,0.9)) as stuckRatioPercentile,
                count(*) as cnt
            from
            (
                select
                    case
                        when (local_type like  '%relay%' or remote_type like '%relay%') then 'relay'
                        else 'p2p' end as type,
                    conn_time,
                    first_frame_time,
                    dec_stuck_time
                from ${APP}.dwd_sdk_su
                where dt='$do_date'
                and conn_time > 0
                and first_frame_time <> -1
            )t1
            group by type
        )t2
        lateral view explode(t2.connTimePercentile) tmp as tmp1
        lateral view explode(t2.fstTimePercentile) tmp as tmp2
        lateral view explode(t2.stuckRatioPercentile) tmp as tmp3
    )t3
)t4
group by type
order by type;"

ads_rtc_cap_send="
insert overwrite table ${APP}.ads_rtc_cap_send
select
    '$do_date',
    type,
    cast(avg(cap_fr_mean) as DECIMAL(16,2)),
    cast(avg(send_fr_mean) as DECIMAL(16,2)),
    count(*)
from
(
    select
        case
            when (local_type like  '%relay%' or remote_type like '%relay%') then 'relay'
            else 'p2p' end as type,
        cap_fr_mean,
        send_fr_mean
    from ${APP}.dwd_sdk_du
    where dt='$do_date'
    and conn_time > 0
    and cap_fr_mean > 0
)t1
group by type
order by type;"

ads_rtc_rcvd_rend="
insert overwrite table ${APP}.ads_rtc_rcvd_rend
select
    '$do_date',
    type,
    cast(avg(rcvd_fr_mean) as DECIMAL(16,2)),
    cast(avg(dec_fr_mean) as DECIMAL(16,2)),
    cast(avg(rend_fr_mean) as DECIMAL(16,2)),
    count(*)
from
(
    select
        case
            when (local_type like  '%relay%' or remote_type like '%relay%') then 'relay'
            else 'p2p' end as type,
        rcvd_fr_mean,
        dec_fr_mean,
        rend_fr_mean
    from ${APP}.dwd_sdk_su
    where dt='$do_date'
    and conn_time > 0
    and rcvd_fr_mean > 0
)t1
group by type
order by type;"

ads_rtc_du_rate="
insert overwrite table ${APP}.ads_rtc_du_rate
select
    '$do_date',
    type,
    concat_ws(',',collect_set(cast(avgTarRate as STRING))),
    concat_ws(',',collect_set(cast(avgActRate as STRING))),
    concat_ws(',',collect_set(cast(avgSendRate as STRING))),
    concat_ws(',',collect_set(cast(tmp1 as STRING))),
    concat_ws(',',collect_set(cast(tmp2 as STRING))),
    concat_ws(',',collect_set(cast(tmp3 as STRING))),
    concat_ws(',',collect_set(cast(cnt as STRING)))
from
(
    select
        type,
        avgTarRate,
        avgActRate,
        avgSendRate,
        cast(tmp1 as BIGINT) tmp1,
        cast(tmp2 as BIGINT) tmp2,
        cast(tmp3 as BIGINT) tmp3,
        cnt
    from
    (
        select
            type,
            cast(avg(tar_rate_mean) as BIGINT) as avgTarRate,
            cast(avg(act_rate_mean) as BIGINT) as avgActRate,
            cast(avg(send_rate_mean) as BIGINT) as avgSendRate,
            percentile_approx(cast(tar_rate_mean as BIGINT), array(0.1,0.3,0.5,0.7,0.9)) as tarRatePercentile,
            percentile_approx(cast(act_rate_mean as BIGINT), array(0.1,0.3,0.5,0.7,0.9)) as actRatePercentile,
            percentile_approx(cast(send_rate_mean as BIGINT), array(0.1,0.3,0.5,0.7,0.9)) as sendRatePercentile,
            count(*) as cnt
        from
        (
            select
                case
                    when (local_type like  '%relay%' or remote_type like '%relay%') then 'relay'
                    else 'p2p' end as type,
                tar_rate_mean,
                act_rate_mean,
                send_rate_mean
            from ${APP}.dwd_sdk_du
            where dt='$do_date'
            and conn_time > 0
            and cap_fr_mean > 0
            and send_rate_mean < 100000
            and act_rate_mean < 100000
        )t1
        group by type
    )t2
    lateral view explode(t2.tarRatePercentile) tmp as tmp1
    lateral view explode(t2.actRatePercentile) tmp as tmp2
    lateral view explode(t2.sendRatePercentile) tmp as tmp3
)t3
group by type
order by type;"

ads_rtc_rtt="
insert overwrite table ${APP}.ads_rtc_rtt
select
    '$do_date',
    type,
    concat_ws(',',collect_set(cast(avg_rtt_max as STRING))),
    concat_ws(',',collect_set(cast(avg_rtt_min as STRING))),
    concat_ws(',',collect_set(cast(avg_rtt_mean as STRING))),
    concat_ws(',',collect_set(cast(tmp_rtt_max_percentile as STRING))),
    concat_ws(',',collect_set(cast(tmp_rtt_min_percentile as STRING))),
    concat_ws(',',collect_set(cast(tmp_rtt_mean_percentile as STRING))),
    concat_ws(',',collect_set(cast(cnt as STRING)))
from
(
   select
        type,
        avg_rtt_max,
        avg_rtt_min,
        avg_rtt_mean,
        case
            when co=1 then concat('0.1:',tmp1)
            when co=26 then concat('0.3:',tmp1)
            when co=51 then concat('0.5:',tmp1)
            when co=76 then concat('0.7:',tmp1)
            when co=101 then concat('0.9:',tmp1)
        end tmp_rtt_max_percentile,
        case
            when co=1 then concat('0.1:',tmp2)
            when co=6 then concat('0.3:',tmp2)
            when co=11 then concat('0.5:',tmp2)
            when co=16 then concat('0.7:',tmp2)
            when co=21 then concat('0.9:',tmp2)
        end tmp_rtt_min_percentile,
        case
            when co=1 then concat('0.1:',tmp3)
            when co=2 then concat('0.3:',tmp3)
            when co=3 then concat('0.5:',tmp3)
            when co=4 then concat('0.7:',tmp3)
            when co=5 then concat('0.9:',tmp3)
        end tmp_rtt_mean_percentile,
        cnt
   from
   (
         select
            type,
            avg_rtt_max,
            avg_rtt_min,
            avg_rtt_mean,
            cast(tmp1 as BIGINT) tmp1,
            cast(tmp2 as BIGINT) tmp2,
            cast(tmp3 as BIGINT) tmp3,
            cnt,
            count(*) over(partition by type rows between UNBOUNDED PRECEDING AND CURRENT ROW ) co
        from
        (
            select
                type,
                cast(avg(rtt_max) as BIGINT) as avg_rtt_max,
                cast(avg(rtt_min) as BIGINT) as avg_rtt_min,
                cast(avg(rtt_mean) as BIGINT) as avg_rtt_mean,
                percentile_approx(cast(rtt_max as BIGINT), array(0.1,0.3,0.5,0.7,0.9)) as rtt_max_percentile,
                percentile_approx(cast(rtt_min as BIGINT), array(0.1,0.3,0.5,0.7,0.9)) as rtt_min_percentile,
                percentile_approx(cast(rtt_mean as BIGINT), array(0.1,0.3,0.5,0.7,0.9)) as rtt_mean_percentile,
                count(*) as cnt
            from
            (
                select
                    case
                        when (local_type like  '%relay%' or remote_type like '%relay%') then 'relay'
                        else 'p2p' end as type,
                    rtt_max,
                    rtt_min,
                    rtt_mean
                from ${APP}.dwd_sdk_du
                where dt='$do_date'
                and conn_time > 0
                and cap_fr_mean > 0
            )t1
            group by type
        )t2
        lateral view explode(t2.rtt_max_percentile) tmp as tmp1
        lateral view explode(t2.rtt_min_percentile) tmp as tmp2
        lateral view explode(t2.rtt_mean_percentile) tmp as tmp3
   )t3
)t4
group by type
order by type;"

ads_rtc_loss="
insert overwrite table ${APP}.ads_rtc_loss
select
    '$do_date',
    type,
    concat_ws(',',collect_set(cast(avg_loss_max as STRING))),
    concat_ws(',',collect_set(cast(avg_loss_mean as STRING))),
    concat_ws(',',collect_set(cast(tmp_loss_max_percentile as STRING))),
    concat_ws(',',collect_set(cast(loss_mean_percentile as STRING))),
    concat_ws(',',collect_set(cast(cnt as STRING)))
from
(
   select
        type,
        avg_loss_max,
        avg_loss_mean,
        case
            when co=1 then concat('0.1:',tmp1)
            when co=6 then concat('0.3:',tmp1)
            when co=11 then concat('0.5:',tmp1)
            when co=16 then concat('0.7:',tmp1)
            when co=21 then concat('0.9:',tmp1)
        end tmp_loss_max_percentile,
        case
            when co=1 then concat('0.1:',tmp2)
            when co=2 then concat('0.3:',tmp2)
            when co=3 then concat('0.5:',tmp2)
            when co=4 then concat('0.7:',tmp2)
            when co=5 then concat('0.9:',tmp2)
        end loss_mean_percentile,
        cnt
   from
   (
         select
            type,
            avg_loss_max,
            avg_loss_mean,
            cast(tmp1 as DECIMAL(16,4)) tmp1,
            cast(tmp2 as DECIMAL(16,4)) tmp2,
            cnt,
            count(*) over(partition by type rows between UNBOUNDED PRECEDING AND CURRENT ROW ) co
        from
        (
            select
                type,
                cast(avg(loss_max) as DECIMAL(16,2)) as avg_loss_max,
                cast(avg(loss_mean) as DECIMAL(16,2)) as avg_loss_mean,
                percentile_approx(cast(loss_max as DECIMAL(16,2) ), array(0.1,0.3,0.5,0.7,0.9),100) as loss_max_percentile,
                percentile_approx(cast(loss_mean as DECIMAL(16,2)), array(0.1,0.3,0.5,0.7,0.9),100) as loss_mean_percentile,
                count(*) as cnt
            from
            (
                select
                    case
                        when (local_type like  '%relay%' or remote_type like '%relay%') then 'relay'
                        else 'p2p' end as type,
                    loss_max,
                    loss_mean
                from ${APP}.dwd_sdk_du
                where dt='$do_date'
                and conn_time > 0
                and cap_fr_mean > 0
            )t1
            group by type
        )t2
        lateral view explode(t2.loss_max_percentile) tmp as tmp1
        lateral view explode(t2.loss_mean_percentile) tmp as tmp2
   )t3
)t4
group by type
order by type;"

ads_bus_new_user="
insert overwrite table ${APP}.ads_bus_new_user
select
    '$do_date',
    bus,
    count(distinct(id))
from ${APP}.dwt_bus_user_topic
where dt = '$do_date'
and first_login_date='$do_date'
group by bus;"

ads_bus_user_start="
insert overwrite table ${APP}.ads_bus_user_start
select
    '$do_date',
    bus,
    action,
    count(distinct(id))
from ${APP}.dwt_bus_user_topic
where dt= '$do_date'
and (last_start_front_date = '$do_date' or last_start_process_date = '$do_date')
group by bus,action;"

ads_bus_user_retention="
insert overwrite table ${APP}.ads_bus_user_retention
select * from ${APP}.ads_bus_user_retention
union
select *
from
(
    select
        '$do_date',
        first_login_date d1,
        bus,
        action,
        datediff('$do_date',first_login_date) c1,
        sum(if(last_start_front_date='$do_date',1,0)),
        sum(if(last_start_process_date='$do_date',1,0)),
        count(*),
        cast(sum(if(last_start_front_date='$do_date',1,0))/count(*)*100 as decimal(16,2)),
        cast(sum(if(last_start_process_date='$do_date',1,0))/count(*)*100 as decimal(16,2))
    from ${APP}.dwt_bus_user_topic
    where dt='$do_date'
    and bus <> ''
    and action <> '' 
    and first_login_date>=date_add('$do_date',-30)
    and first_login_date<'$do_date'
    group by first_login_date,action,bus
)t1
where t1.c1 in(1,2,3,4,5,6,7,14,30);"


ads_new_user="
insert overwrite table ${APP}.ads_new_user
select
    '$do_date',
    count(distinct(id)) c1
from ${APP}.dwt_user_topic
where dt = '$do_date'
and first_login_date='$do_date'
order by c1 desc ;"

ads_start_user="
insert overwrite table ${APP}.ads_start_user
select
    '$do_date',
    action,
    count(1) 
from ${APP}.dwt_user_topic
where dt= '$do_date'
and (last_start_front_date = '$do_date' or last_start_process_date = '$do_date') 
group by action;"

ads_user_retention="
insert overwrite table ${APP}.ads_user_retention
select * from ${APP}.ads_user_retention
union
select *
from
(
    select
        '$do_date',
        first_login_date d1,
        action,
        datediff('$do_date',first_login_date) c1, 
        sum(if(last_start_front_date='$do_date',1,0)),
        sum(if(last_start_process_date='$do_date',1,0)),
        count(*),
        cast(sum(if(last_start_front_date='$do_date',1,0))/count(*)*100 as decimal(16,2)),
        cast(sum(if(last_start_process_date='$do_date',1,0))/count(*)*100 as decimal(16,2))
    from ${APP}.dwt_user_topic
    where dt='$do_date'
    and first_login_date>=date_add('$do_date',-30)
    and first_login_date<'$do_date'
    and action <> ''
    group by first_login_date,action 
)t1
where t1.c1 in(1,2,3,4,5,6,7,14,30);"

ads_client_new_mac="
insert overwrite table ${APP}.ads_client_new_mac
select
    '$do_date',
    client_type,
    count(1) c1
from ${APP}.dwt_mac_topic
where dt = '$do_date'
and first_login_date='$do_date'
group by client_type
order by c1 desc;"

ads_client_start_mac="
insert overwrite table ${APP}.ads_client_start_mac
select
    '$do_date',
    client_type,
    action_type,
    count(1)
from ${APP}.dwt_mac_topic
where dt= '$do_date'
and (last_start_front_date = '$do_date' or last_start_process_date = '$do_date') 
group by client_type,action_type;"

ads_mac_retention="
insert overwrite table ${APP}.ads_mac_retention
select * from ${APP}.ads_mac_retention
union
select *
from
(
    select
        '$do_date',
        first_login_date d1,
        client_type,
        action_type,
        datediff('$do_date',first_login_date) c1, 
        sum(if(last_start_front_date='$do_date',1,0)),
        sum(if(last_start_process_date='$do_date',1,0)),
        count(*), 
        cast(sum(if(last_start_front_date='$do_date',1,0))/count(*)*100 as decimal(16,2)),
        cast(sum(if(last_start_process_date='$do_date',1,0))/count(*)*100 as decimal(16,2))
    from ${APP}.dwt_mac_topic
    where dt='$do_date'
    and first_login_date>=date_add('$do_date',-30)
    and first_login_date<'$do_date'
    and action_type <> ''
    group by first_login_date,client_type,action_type 
)t1
where t1.c1 in(1,2,3,4,5,6,7,14,30);"



ads_client_new_su="
insert overwrite table ${APP}.ads_client_new_su
select
    '$do_date',
    client_type,
    count(1) c1
from ${APP}.dwt_su_topic
where dt = '$do_date'
and first_login_date='$do_date'
group by client_type
order by c1 desc;"

ads_client_start_su="
insert overwrite table ${APP}.ads_client_start_su
select
    '$do_date',
    client_type,
    action_type,
    count(1)
from ${APP}.dwt_su_topic
where dt= '$do_date'
and (last_start_front_date = '$do_date' or last_start_process_date = '$do_date') 
group by client_type,action_type;"

ads_su_retention="
insert overwrite table ${APP}.ads_su_retention
select * from ${APP}.ads_su_retention
union
select *
from
(
    select
        '$do_date',
        first_login_date d1,
        client_type,
        action_type,
        datediff('$do_date',first_login_date) c1, 
        sum(if(last_start_front_date='$do_date',1,0)),
        sum(if(last_start_process_date='$do_date',1,0)),
        count(*), 
        cast(sum(if(last_start_front_date='$do_date',1,0))/count(*)*100 as decimal(16,2)),
        cast(sum(if(last_start_process_date='$do_date',1,0))/count(*)*100 as decimal(16,2))
    from ${APP}.dwt_su_topic
    where dt='$do_date'
    and first_login_date>=date_add('$do_date',-30)
    and first_login_date<'$do_date'
    and action_type <> ''
    group by first_login_date,client_type,action_type 
)t1
where t1.c1 in(1,2,3,4,5,6,7,14,30);"

ads_client_new_du="
insert overwrite table ${APP}.ads_client_new_du
select
    '$do_date',
    client_type,
    count(1) c1
from ${APP}.dwt_du_topic
where dt = '$do_date'
and first_login_date='$do_date'
group by client_type
order by c1 desc;"

ads_client_start_du="
insert overwrite table ${APP}.ads_client_start_du
select
    '$do_date',
    client_type,
    action_type,
    count(1)
from ${APP}.dwt_du_topic
where dt= '$do_date'
and (last_start_front_date = '$do_date' or last_start_process_date = '$do_date') 
group by client_type,action_type;"

ads_du_retention="
insert overwrite table ${APP}.ads_du_retention
select * from ${APP}.ads_du_retention
union
select *
from
(
    select
        '$do_date',
        first_login_date d1,
        client_type,
        action_type,
        datediff('$do_date',first_login_date) c1, 
        sum(if(last_start_front_date='$do_date',1,0)),
        sum(if(last_start_process_date='$do_date',1,0)),
        count(*), 
        cast(sum(if(last_start_front_date='$do_date',1,0))/count(*)*100 as decimal(16,2)),
        cast(sum(if(last_start_process_date='$do_date',1,0))/count(*)*100 as decimal(16,2))
    from ${APP}.dwt_du_topic
    where dt='$do_date'
    and first_login_date>=date_add('$do_date',-30)
    and first_login_date<'$do_date'
    and action_type <> ''
    group by first_login_date,client_type,action_type 
)t1
where t1.c1 in(1,2,3,4,5,6,7,14,30);"

ads_version_mac_total="
insert overwrite table ${APP}.ads_version_mac_total
select
    '$do_date',
    version,
    count(distinct(mac_id))
from ${APP}.dwd_client_event
where dt='$do_date'
and action='start_process'
and version>='3.0.0'
group by version;"

ads_su_handshake_total="
insert overwrite table ${APP}.ads_su_handshake_total
select
    '$do_date',
    t1.version,
    sum(if(success_connid is not null,1,0)),
    count(1),
    cast(sum(if(success_connid is not null,1,0)) / count(1) * 100 as DECIMAL(16,2))
from
(
    select connid,
           version
    from ${APP}.dwd_handshake_sd
    where dt='$do_date'
    and type='controller'
    and client='PC'
    group by connid,version
)t1
left join
(
    select
           connid success_connid
    from ${APP}.dwd_handshake_success
    where dt='$do_date'
    and sclient='PC'
    group by connid
)t2
on t1.connid=t2.success_connid
group by t1.version;"

ads_du_handshake_total="
insert overwrite table ${APP}.ads_du_handshake_total
select
    '$do_date',
    t1.version,
    sum(if(success_connid is not null,1,0)),
    count(1),
    cast(sum(if(success_connid is not null,1,0)) / count(1) * 100 as DECIMAL(16,2))
from
(
    select connid,
           version
    from ${APP}.dwd_handshake_sd
    where dt='$do_date'
    and type='presenter'
    and client='PC'
    group by connid,version
)t1
left join
(
    select
           connid success_connid
    from ${APP}.dwd_handshake_success
    where dt='$do_date'
    and sclient='PC'
    group by connid
)t2
on t1.connid=t2.success_connid
group by t1.version;"

ads_su_handshake_failure="
insert overwrite table ${APP}.ads_su_handshake_failure
select
    *
from
(
    select
        '$do_date',
        action,
        version,
        get_json_object(errno,'$.errno') err,
        count(1)
    from ${APP}.dwd_client_tevent
    where dt='$do_date'
    and action in ('conerr','conful')
    and version>'4.2.1'
    and eventtype='tran'
    and clientype='0'
    group by action,version,get_json_object(errno,'$.errno')
)t1
where t1.err is not null;"

ads_du_handshake_failure="
insert overwrite table ${APP}.ads_du_handshake_failure
select
    *
from
(
    select
        '$do_date',
        action,
        version,
        get_json_object(errno,'$.errno') err,
        count(1)
    from ${APP}.dwd_client_tevent
    where dt='$do_date'
    and action in ('conerr','conful')
    and version>'4.2.1'
    and eventtype='tran'
    and clientype='1'
    group by action,version,get_json_object(errno,'$.errno')
)t1
where t1.err is not null;"

ads_user_login_total="
insert overwrite table ${APP}.ads_user_login_total
select
    '$do_date',
    t4.type,
    t4.enterprise,
    t4.total_count,
    t5.error_count,
    cast(t5.error_count/t4.total_count *100 as DECIMAL(16,2))
from
(
    select
        'accountLogin' type,
        t1.enterprise ,
        count(1) total_count
    from
    (
        select
            case
                when enterprise is null or enterprise='0' then 'consumer'
                when enterprise = '1' then 'business'
            end enterprise
        from ${APP}.dwd_user_login
        where dt='$do_date'
    )t1
    group by enterprise
    union all
    select
        'wechatLogin',
        'consumer',
        count(1)
    from ${APP}.dwd_wechat_login
    where dt='$do_date'
    union all
    select
        'phoneLogin',
        'consumer',
        count(1)
    from ${APP}.dwd_phone_login
    where dt='$do_date'
    union all
    select
        'qrcodeLogin',
        'consumer',
        count(1)
    from ${APP}.dwd_qrcode_login
    where dt='$do_date'
    union all
    select
    case
        when logintype='1' then 'wechatAuto'
        when logintype='2' then 'phoneAuto'
        when logintype='3' then 'APPAuto'
        when logintype='4' then 'iosAuto'
    end logintype,
    'consumer',
    count(1)
from ${APP}.dwd_auto_login
where dt='$do_date'
and logintype in ('1','2','3','4')
group by logintype
)t4
left join
(
    select
        'accountLogin' type,
        t2.enterprise,
        count(1) error_count
    from
    (
        select
            case
                when enterprise is null or enterprise='0' then 'consumer'
                when enterprise = '1' then 'business'
            end enterprise
        from ${APP}.dwd_user_login
        where dt='$do_date'
        and result='0'
    )t2
    group by enterprise
    union all
    select
        t3.url,
        'consumer',
        count(1)
    from
    (
        select
            case
                when ck_url rlike '/api/login/wechatUser.*' then  'wechatLogin'
                when ck_url='/api/qrcode/login' then  'qrcodeLogin'
                when ck_url='/api/phone/verifyPhoneCode' then  'phoneLogin'
                when ck_url='/api/qrcode/autoLogin' and get_json_object(get_json_object(passbody,'$.user'),'$.logintype') = '1' then  'wechatAuto'
                when ck_url='/api/qrcode/autoLogin' and get_json_object(get_json_object(passbody,'$.user'),'$.logintype') = '2' then  'phoneAuto'
                when ck_url='/api/qrcode/autoLogin' and get_json_object(get_json_object(passbody,'$.user'),'$.logintype') = '3' then  'APPAuto'
                when ck_url='/api/qrcode/autoLogin' and get_json_object(get_json_object(passbody,'$.user'),'$.logintype') = '4' then  'iosAuto'
            end url
        from ${APP}.dwd_login_error
        where dt='$do_date'
    )t3
    where t3.url is not null
    group by t3.url
)t5
on t4.type=t5.type and t4.enterprise=t5.enterprise;"

ads_vendor_process_total="
insert overwrite table ${APP}.ads_vendor_process_total
select
    '$do_date',
    vendor,
    count(distinct(mac_id))
from ${APP}.dwd_client_event
where dt='$do_date'
and platform='windows'
and action='start_process'
group by vendor;"


ads_windows_first_frame_total="
insert overwrite table ${APP}.ads_windows_first_frame_total
select
    '$do_date',
    version,
    forward,
    fail,
    c1,
    cast(c1/sum(c1) over(partition by version,forward,action) as DECIMAL(16,2) )
from
(
    select
        version,
        forward,
        action,
        fail,
        count(1) c1 
    from
    (
        select
            version,
            action,
            case
                when action='fail' then 'fail'
                when action='success' and (version='4.2.5' or version >= '4.2.8') and cast(get_json_object(get_json_object(ck_url,'$.connectDuration'),'$.time') as BIGINT) <= 200 then '200-'
                when action='success' and (version='4.2.5' or version >= '4.2.8') and cast(get_json_object(get_json_object(ck_url,'$.connectDuration'),'$.time') as BIGINT) > 200 and cast(get_json_object(get_json_object(ck_url,'$.connectDuration'),'$.time') as BIGINT) <= 500 then '500-'
                when action='success' and (version='4.2.5' or version >= '4.2.8') and cast(get_json_object(get_json_object(ck_url,'$.connectDuration'),'$.time') as BIGINT) > 500 and cast(get_json_object(get_json_object(ck_url,'$.connectDuration'),'$.time') as BIGINT) <= 1000 then '1000-'
                when action='success' and (version='4.2.5' or version >= '4.2.8') and cast(get_json_object(get_json_object(ck_url,'$.connectDuration'),'$.time') as BIGINT) > 1000 then '1000+'
                when action='success' and (version<'4.2.5' or (version > '4.2.5' and version < '4.2.8' ))  and cast(get_json_object(ck_url,'$.connectDuration') as BIGINT) <= 200 then '200-'
                when action='success' and (version<'4.2.5' or (version > '4.2.5' and version < '4.2.8' )) and cast(get_json_object(ck_url,'$.connectDuration') as BIGINT) > 200 and cast(get_json_object(ck_url,'$.connectDuration') as BIGINT) <= 500 then '500-'
                when action='success' and (version<'4.2.5' or (version > '4.2.5' and version < '4.2.8' )) and cast(get_json_object(ck_url,'$.connectDuration') as BIGINT) > 500 and cast(get_json_object(ck_url,'$.connectDuration') as BIGINT) <= 1000 then '1000-'
                when action='success' and (version<'4.2.5' or (version > '4.2.5' and version < '4.2.8' )) and cast(get_json_object(ck_url,'$.connectDuration') as BIGINT) > 1000 then '1000+'
            end fail,
            case
                when (version='4.2.5' or version >= '4.2.8') and action='success' then get_json_object(get_json_object(ck_url,'$.connectDuration'),'$.link')
                when (version='4.2.5' or version >= '4.2.8') and action='fail' then get_json_object(get_json_object(ck_url,'$.connectFailReason'),'$.link')
                when (version<'4.2.5' or (version > '4.2.5' and version < '4.2.8' )) then '-'
            end forward
        from ${APP}.dwd_client_event
        where dt='$do_date'
        and platform='windows'
        and page='client'
        and action in ('success','fail')
    )t1
    group by version,forward,action,fail
)t2
where t2.fail is not null
and t2.forward is not null;"

ads_android_action_total="
insert overwrite table ${APP}.ads_android_action_total
select
    '$do_date',
    platform,
    if(version is null,'all',version),
    action,
    count1,
    count2
from
(
    select
        platform,
        version,
        action,
        case
            when action ='stuck_ratio_rate' then sum(if(turndata_result_code='0',1,0))
            else  sum(if(turndata_result_code='1',1,0))
        end count1,
        case
            when action ='stuck_ratio_rate' then count(1)
            when action ='udx_success_rate' then sum(if(turndata_result_code='1' or turndata_result_code='0',1,0))
            else  sum(if(turndata_result_code='1' or turndata_result_code='-1',1,0))
        end count2
    from
    (
        select
            case
                when action='8' then 'upnp_success_rate'
                when action='10' then 'udx_success_rate'
                when action='16' then 'forward_success_rate'
                when action='18' then 'stuck_ratio_rate'
            end action,
            platform,
            version,
            turndata_result_code
        from ${APP}.dwd_client_tuevent
        where dt='$do_date'
        and events='link_tech'
        and platform in ('android','iOS')
        and action in ('8','10','16','18')
    )t1
    group by action,version,platform grouping sets((action,platform),(action,platform,version))
    union all
    select
        platform,
        version,
        'logic_success_rate',
        sum(if(action='21' and turndata_result_code='1',1,0)),
        sum(if(action='20',1,0))
    from ${APP}.dwd_client_tuevent
    where dt='$do_date'
    and events='link_tech'
    and platform in ('android','iOS')
    and action in ('20','21')
    and version>='4.4.3'
    group by platform,version grouping sets(platform,(platform,version))
)t2;"

ads_android_action_pvuv="
insert overwrite table ${APP}.ads_android_action_pvuv
select
    '$do_date',
    platform,
    version,
    action,
    count(1),
    sum(c1)
from
(
    select
        platform,
        version,
        case
            when action='7' then 'upnp'
            when action='10' then 'udx'
        end action,
        count(1) c1
    from ${APP}.dwd_client_tuevent
    where dt='$do_date'
    and events='link_tech'
    and platform in ('android','iOS')
    and action in ('7','10')
    group by platform,version,id,action
)t1
group by platform,version,action;"

ads_android_du_total="
insert overwrite table ${APP}.ads_android_du_total
select
    '$do_date',
    version,
    cast(sum(if(action='23' and turndata_result_code = '1',1,0)) / sum(if(action='22',1,0)) * 100 as DECIMAL(16,2)),
    cast(sum(if(action='23' and turndata_result_code = '2',1,0)) / sum(if(action='22',1,0)) * 100 as DECIMAL(16,2)),
    cast(sum(if(action='27',1,0)) / sum(if(action='26',1,0)) * 100 as DECIMAL(16,2))
from ${APP}.dwd_client_tuevent
where dt = '$do_date'
and events='link_tech'
and platform='android'
and action in('22','23','26','27')
group by version;"

ads_ip_user_login_total="
insert overwrite table ${APP}.ads_ip_user_login_total
select
    '$do_date',
    'accountLogin',
    t1.enterprise,
    count(distinct(sip))
from
(
    select
        sip,
        case
            when enterprise is null or enterprise='0' then 'consumer'
            when enterprise = '1' then 'business'
        end enterprise
    from ${APP}.dwd_user_login
    where dt='$do_date'
)t1
group by enterprise;"

ads_android_setting_total="
insert overwrite table ${APP}.ads_android_setting_total
select
    '$do_date',
    version,
    platform,
    cast(sum(if(action='4',1,0)) / sum(if(action='1',1,0)) * 100 AS DECIMAL(16,2))
from ${APP}.dwd_client_tuevent
where dt='$do_date'
and events='setting_tech'
and platform is not null
and action in ('1','4')
group by platform,version;"

ads_android_test_double="
insert overwrite table ${APP}.ads_android_test_double
select
    '$do_date',
    turndata_test_double,
    count(distinct(id))
from ${APP}.dwd_client_tuevent
where dt='$do_date'
and events='setting_tech'
and platform='android'
and action='3'
and turndata_test_double is not null
group by turndata_test_double;"

ads_client_version_new_su="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_client_version_new_su
select
    '$do_date',
    client_type,
    sum(if(first_su_date='$do_date',1,0)),
    sum(if(last_su_date='$do_date',1,0))
from ${APP}.dwt_su_total_topic
where dt = '$do_date'
and client_type is not null 
group by client_type;"

ads_client_version_retention="
insert overwrite table ${APP}.ads_client_version_retention
select * from ${APP}.ads_client_version_retention
union
select *
from
(
    select
        '$do_date',
        first_su_date d1,
        client_type,
        datediff('$do_date',first_su_date) c1, 
        sum(if(last_su_date='$do_date',1,0)),
        count(*) 
    from ${APP}.dwt_su_total_topic
    where dt='$do_date'
    and first_su_date>=date_add('$do_date',-30)
    and first_su_date<'$do_date'
    group by first_su_date,client_type  
)t1
where t1.c1 in(1,2,3,4,5,6,7,14,30);"

ads_client_version_du_total="
insert overwrite table ${APP}.ads_client_version_du_total
select
    '$do_date',
    client_type,
    sum(if(first_du_date='$do_date',1,0)),
    sum(if(last_du_date='$do_date',1,0))
from ${APP}.dwt_du_total_topic
where dt = '$do_date'
and client_type is not null
group by client_type;"

ads_client_version_du_retention="
insert overwrite table ${APP}.ads_client_version_du_retention
select * from ${APP}.ads_client_version_du_retention
union
select *
from
(
    select
        '$do_date',
        first_du_date d1,
        client_type,
        datediff('$do_date',first_du_date) c1, 
        sum(if(last_du_date='$do_date',1,0)),
        count(*) 
    from ${APP}.dwt_du_total_topic
    where dt='$do_date'
    and first_du_date>=date_add('$do_date',-30)
    and first_du_date<'$do_date'
    group by first_du_date,client_type  
)t1
where t1.c1 in(1,2,3,4,5,6,7,14,30);"

ads_client_logic_total="
insert overwrite table ${APP}.ads_client_logic_total
select
    '$do_date',
    client_type,
    sum(if(first_logic_date='$do_date',1,0)),
    sum(if(last_logic_date='$do_date',1,0))
from ${APP}.dwt_client_topic
where dt = '$do_date'
and client_type is not null
group by client_type;"

ads_client_logic_retention="
insert overwrite table ${APP}.ads_client_logic_retention
select * from ${APP}.ads_client_logic_retention
union
select *
from
(
    select
        '$do_date',
        first_logic_date d1,
        client_type,
        datediff('$do_date',first_logic_date) c1, 
        sum(if(last_logic_date='$do_date',1,0)),
        count(*) 
    from ${APP}.dwt_client_topic
    where dt='$do_date'
    and first_logic_date>=date_add('$do_date',-30)
    and first_logic_date<'$do_date'
    group by first_logic_date,client_type  
)t1
where t1.c1 in(1,2,3,4,5,6,7,14,30);"

ads_rtc_version_conn_total="
insert overwrite table ${APP}.ads_rtc_version_conn_total
select '$do_date',
       version,
       type,
       c1,
       cast(c1 / sum(c1) over (partition by version) * 100 as DECIMAL(16, 2))
from (
         select if(version is null,'total',version) version,
                if(type is null,'total',type) type,
                count(1) c1
         from (
                  select conn_id,
                         version,
                         case
                             when is_rtc = '1' and conn_type = '0' then 'erizo'
                             when is_rtc = '1' and conn_type = '1' then 'p2p'
                             when is_rtc = '1' and conn_type = '2' then 'sfu'
                             when is_rtc = '0' then 'fallback'
                             else 'other' end as type
                  from ${APP}.dwd_sdk_su
                  where dt = '$do_date'
                    and over_num = success_num
                    and version is not null
                  union
                  select conn_id,
                         version,
                         case
                             when is_rtc = '1' and conn_type = '0' then 'erizo'
                             when is_rtc = '1' and conn_type = '1' then 'p2p'
                             when is_rtc = '1' and conn_type = '2' then 'sfu'
                             when is_rtc = '0' then 'fallback'
                             else 'other' end as type
                  from ${APP}.dwd_sdk_du
                  where dt = '$do_date'
                    and over_num = success_num
                    and version is not null
              ) t1
         group by version, type grouping sets ((version,type),type,())
     ) t2;"

ads_rtc_version_vedio_total="
insert overwrite table ${APP}.ads_rtc_version_vedio_total
select '$do_date',
       if(t2.version is null,'total',t2.version),
       if(t2.type is null,'total',t2.type),
       if(t2.local_proto is null,'total',t2.local_proto),
       vedio_count,
       total_count,
       cast(vedio_count / total_count * 100 as DECIMAL(16, 2))
from
(
     select
        version,
        type,
        local_proto,
        sum(if(cast(first_frame_time as BIGINT) > 0, 1, 0)) vedio_count,
        count(1)                                            total_count
     from (
              select version,
                     if(local_proto is null,'forward',local_proto) local_proto,
                     case
                         when ((version <'4.5.0.0' and is_rtc = '1') or (version >='4.5.0.0' and \`rollback\`='0'))  and conn_type = '0' then 'erizo'
                         when ((version <'4.5.0.0' and is_rtc = '1') or (version >='4.5.0.0' and \`rollback\`='0')) and conn_type = '1' then 'p2p'
                         when ((version <'4.5.0.0' and is_rtc = '1') or (version >='4.5.0.0' and \`rollback\`='0')) and conn_type = '2' then 'sfu'
                         when ((version <'4.5.0.0' and is_rtc = '0') or (version >='4.5.0.0' and \`rollback\`='1')) then 'fallback'
                         else 'other' end as type,
                     first_frame_time
              from ${APP}.dwd_sdk_su
              where dt = '$do_date'
                and version is not null
                and over_num = success_num
          ) t1
     group by t1.version, type ,local_proto grouping sets ((t1.version, type ,local_proto),(t1.version, local_proto),())
)t2;"

ads_rtc_version_fallback_total="
insert overwrite table ${APP}.ads_rtc_version_fallback_total
select
    '$do_date',
    version,
    cast(sum(if(((version <'4.5.0.0' and is_rtc = '0') or (version >='4.5.0.0' and \`rollback\`='1')),1,0))/count(1) * 100 AS DECIMAL(16,2)),
    cast(sum(if(((version <'4.5.0.0' and is_rtc = '0') or (version >='4.5.0.0' and \`rollback\`='1')) and cast(first_frame_time as BIGINT) > 0,1,0)) / count(1) * 100 AS DECIMAL(16,2) ),
    cast(sum(if(((version <'4.5.0.0' and is_rtc = '0') or (version >='4.5.0.0' and \`rollback\`='1')),1,0))/count(1) * 100 AS DECIMAL(16,2))-cast(sum(if(((version <'4.5.0.0' and is_rtc = '0') or (version >='4.5.0.0' and \`rollback\`='1')) and cast(first_frame_time as BIGINT) > 0,1,0)) / count(1) * 100 AS DECIMAL(16,2) )
from ${APP}.dwd_sdk_su
where dt='$do_date'
and over_num=success_num
and version is not null
group by version;"

ads_rtc_version_conn_fst_time="
insert overwrite table ${APP}.ads_rtc_version_conn_fst_time
select '$do_date',
       if(version is null,'total',version),
       if(t2.type is null,'total',t2.type),
       if(t2.local_proto is null,'total',t2.local_proto),
       avgConnTime,
       maxConnTime,
       concat_ws(',', cast(cast(connTimePercentile[0] as BIGINT) AS String),
                 cast(cast(connTimePercentile[1] as BIGINT) AS String),
                 cast(cast(connTimePercentile[2] as BIGINT) AS String),
                 cast(cast(connTimePercentile[3] as BIGINT) AS String),
                 cast(cast(connTimePercentile[4] as BIGINT) AS String)),
       cast(connTimePercentile[0] as BIGINT),
       cast(connTimePercentile[1] as BIGINT),
       cast(connTimePercentile[2] as BIGINT),
       cast(connTimePercentile[3] as BIGINT),
       cast(connTimePercentile[4] as BIGINT),
       avgFstTime,
       maxFstTime,
       concat_ws(',', cast(cast(fstTimePercentile[0] as BIGINT) AS String),
                 cast(cast(fstTimePercentile[1] as BIGINT) AS String),
                 cast(cast(fstTimePercentile[2] as BIGINT) AS String),
                 cast(cast(fstTimePercentile[3] as BIGINT) AS String),
                 cast(cast(fstTimePercentile[4] as BIGINT) AS String)),
       cast(fstTimePercentile[0] as BIGINT),
       cast(fstTimePercentile[1] as BIGINT),
       cast(fstTimePercentile[2] as BIGINT),
       cast(fstTimePercentile[3] as BIGINT),
       cast(fstTimePercentile[4] as BIGINT),
       avgStuckRatio,
	c1
from (
         select version,
                type,
                local_proto,
                cast(avg(conn_time) as BIGINT)                    as                   avgConnTime,
                max(cast(conn_time as BIGINT))                    as                   maxConnTime,
                percentile_approx(cast(conn_time as BIGINT),
                                  array(0.1, 0.3, 0.5, 0.7, 0.9)) as                   connTimePercentile,
                cast(avg(first_frame_time) as BIGINT)             as                   avgFstTime,
                max(cast(first_frame_time as BIGINT))             as                   maxFstTime,
                percentile_approx(cast(first_frame_time as BIGINT),
                                  array(0.1, 0.3, 0.5, 0.7, 0.9)) as                   fstTimePercentile,
		cast(avg(dec_stuck_time / (1000 * conn_time) * 100) as DECIMAL(16, 2)) avgStuckRatio,
		count(1) c1
         from (
                  select version,
                         if(local_proto is null,'forward',local_proto) local_proto,
                         case
                            when ((version <'4.5.0.0' and is_rtc = '1') or (version >='4.5.0.0' and \`rollback\`='0'))  and conn_type = '0' then 'erizo'
                            when ((version <'4.5.0.0' and is_rtc = '1') or (version >='4.5.0.0' and \`rollback\`='0')) and conn_type = '1' then 'p2p'
                            when ((version <'4.5.0.0' and is_rtc = '1') or (version >='4.5.0.0' and \`rollback\`='0')) and conn_type = '2' then 'sfu'
                            when ((version <'4.5.0.0' and is_rtc = '0') or (version >='4.5.0.0' and \`rollback\`='1')) then 'fallback'
                         else 'other' end as type,
                         conn_time,
                         first_frame_time,
                         dec_stuck_time
                  from ${APP}.dwd_sdk_su
                  where dt = '$do_date'
                    and version is not null
                    and over_num = success_num
                    and cast(conn_time as BIGINT) >= 0
                    and cast(conn_time as BIGINT) < 172800
                    and cast(first_frame_time as BIGINT) > -1
                    and cast(first_frame_time as BIGINT) < 25000
              ) t1
         group by version, type ,local_proto grouping sets ((version, type ,local_proto),(version, local_proto),())
     ) t2;"

ads_rtc_version_cap_send="
insert overwrite table ${APP}.ads_rtc_version_cap_send
select '$do_date',
       if(t1.version is null,'total',t1.version),
       if(t1.type is null,'total',t1.type),
       if(t1.local_proto is null,'total',t1.local_proto),
       cast(avg(cap_fr_mean) as DECIMAL(16, 2)),
       cast(avg(send_fr_mean) as DECIMAL(16, 2)),count(1)
from (
         select
                if(local_proto is null,'forward',local_proto) local_proto,
                case
                    when conn_type = '0' then 'erizo'
                    when conn_type = '1' then 'p2p'
                    when conn_type = '2' then 'sfu'
                    else 'other' end as              type,
                cast(cap_fr_mean as DECIMAL(16, 2))  cap_fr_mean,
                cast(send_fr_mean as DECIMAL(16, 2)) send_fr_mean,
                version
         from ${APP}.dwd_sdk_du
         where dt = '$do_date'
           and version is not null
           and over_num = success_num
           and is_rtc = '1'
           and cast(conn_time as BIGINT) >= 0
           and cast(conn_time as BIGINT) < 172800
           and cast(act_rate_mean as BIGINT) < 100000
     ) t1
group by version, type ,local_proto grouping sets ((version, type ,local_proto),(version, local_proto),());"


ads_rtc_version_rcvd_rend="
insert overwrite table ${APP}.ads_rtc_version_rcvd_rend
select '$do_date',
       if(t1.version is null,'total',t1.version),
       if(t1.type is null,'total',t1.type),
       if(t1.local_proto is null,'total',t1.local_proto),
       cast(avg(rcvd_fr_mean) as DECIMAL(16, 2)),
       cast(avg(dec_fr_mean) as DECIMAL(16, 2)),
       cast(avg(rend_fr_mean) as DECIMAL(16, 2)),count(1)
from (
         select version,
                if(local_proto is null,'forward',local_proto) local_proto,
                case
                    when conn_type = '0' then 'erizo'
                    when conn_type = '1' then 'p2p'
                    when conn_type = '2' then 'sfu'
                    else 'other' end as              type,
                cast(rcvd_fr_mean as DECIMAL(16, 2)) rcvd_fr_mean,
                cast(dec_fr_mean as DECIMAL(16, 2))  dec_fr_mean,
                cast(rend_fr_mean as DECIMAL(16, 2)) rend_fr_mean
         from ${APP}.dwd_sdk_su
         where dt = '$do_date'
           and version is not null
           and over_num = success_num
           and is_rtc = '1'
           and cast(conn_time as BIGINT) >= 0
           and cast(conn_time as BIGINT) < 172800
           and cast(rcvd_rate_mean as BIGINT) < 100000
     ) t1
group by version, type ,local_proto grouping sets ((version, type ,local_proto),(version, local_proto),());"
ads_rtc_version_du_rate="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_rtc_version_du_rate
select '$do_date',
       if(t2.version is null,'total',t2.version),
       if(t2.type is null,'total',t2.type),
       if(t2.local_proto is null,'total',t2.local_proto),
       c1,
       avgTarRate,
       avgActRate,
       avgSendRate,
       encBps,
       headBps,
       paylodBps,
       retxBps,
       concat_ws(',', cast(cast(tarRatePercentile[0] as BIGINT) AS String),
                 cast(cast(tarRatePercentile[1] as BIGINT) AS String),
                 cast(cast(tarRatePercentile[2] as BIGINT) AS String),
                 cast(cast(tarRatePercentile[3] as BIGINT) AS String),
                 cast(cast(tarRatePercentile[4] as BIGINT) AS String)),
       concat_ws(',', cast(cast(actRatePercentile[0] as BIGINT) AS String),
                 cast(cast(actRatePercentile[1] as BIGINT) AS String),
                 cast(cast(actRatePercentile[2] as BIGINT) AS String),
                 cast(cast(actRatePercentile[3] as BIGINT) AS String),
                 cast(cast(actRatePercentile[4] as BIGINT) AS String)),
       concat_ws(',', cast(cast(sendRatePercentile[0] as BIGINT) AS String),
                 cast(cast(sendRatePercentile[1] as BIGINT) AS String),
                 cast(cast(sendRatePercentile[2] as BIGINT) AS String),
                 cast(cast(sendRatePercentile[3] as BIGINT) AS String),
                 cast(cast(sendRatePercentile[4] as BIGINT) AS String))
from (
         select
                if(t1.version is null,'total',t1.version) version,
                if(t1.type is null,'total',t1.type) type,
                if(t1.local_proto is null,'total',t1.local_proto) local_proto,
                cast(avg(cast(tar_rate_mean AS DECIMAL(16, 2))) as BIGINT)                        as avgTarRate,
                cast(avg(cast(act_rate_mean AS DECIMAL(16, 2))) as BIGINT)                        as avgActRate,
                cast(avg(cast(send_rate_mean AS DECIMAL(16, 2))) as BIGINT)                       as avgSendRate,
                cast(avg(1.0 * EBytes * 8 / conn_time / 1000) as BIGINT)                          as encBps,
                cast(avg(1.0 * HBytes * 8 / conn_time / 1000) as BIGINT)                          as headBps,
                cast(avg(1.0 * PBytes * 8 / conn_time / 1000) as BIGINT)                          as paylodBps,
                cast(avg(1.0 * RBytes * 8 / conn_time / 1000) as BIGINT)                          as retxBps,
                percentile_approx(cast(tar_rate_mean as BIGINT), array(0.1, 0.3, 0.5, 0.7, 0.9))  as tarRatePercentile,
                percentile_approx(cast(act_rate_mean as BIGINT), array(0.1, 0.3, 0.5, 0.7, 0.9))  as actRatePercentile,
                percentile_approx(cast(send_rate_mean as BIGINT), array(0.1, 0.3, 0.5, 0.7, 0.9)) as sendRatePercentile,
                count(*)          c1
         from (
                  select version,
                         if(local_proto is null,'forward',local_proto) local_proto,
                         case
                             when conn_type = '0' then 'erizo'
                             when conn_type = '1' then 'p2p'
                             when conn_type = '2' then 'sfu'
                             else 'other' end as type,
                         tar_rate_mean,
                         act_rate_mean,
                         send_rate_mean,
                         EBytes,
                         HBytes,
                         PBytes,
                         RBytes,
                         conn_time
                  from ${APP}.dwd_sdk_du
                  where dt = '$do_date'
                    and version is not null
                    and over_num = success_num
                    and ((version <'4.5.0.0' and is_rtc = '1') or (version >='4.5.0.0' and \`rollback\`='0'))
                    and cast(conn_time as BIGINT) >= 0
                    and cast(conn_time as BIGINT) < 172800
		    and cast(send_rate_max as bigint) < 1000000000
              ) t1
         group by version, type ,local_proto grouping sets ((version, type ,local_proto),(version, local_proto),())
     ) t2;"

ads_rtc_version_rtt="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_rtc_version_rtt
select '$do_date',
       version,
       type,
       local_proto,
       avg_rtt_max,
       avg_rtt_mean,
       avg_rtt_min,
       cast(rtt_std_percentile[0] as DECIMAL(16, 2)),
       cast(rtt_std_percentile[1] as DECIMAL(16, 2)),
       cast(rtt_std_percentile[2] as DECIMAL(16, 2)),
       cast(rtt_std_percentile[3] as DECIMAL(16, 2)),
       cast(rtt_std_percentile[4] as DECIMAL(16, 2)),c1
from (
         select if(t1.version is null,'total',t1.version) version,
                if(t1.type is null,'total',t1.type) type,
                if(t1.local_proto is null,'total',t1.local_proto) local_proto,
                cast(avg(cast(rtt_max as DECIMAL(16, 2))) as DECIMAL(16, 2))                       as avg_rtt_max,
                cast(avg(cast(rtt_min as DECIMAL(16, 2))) as DECIMAL(16, 2))                       as avg_rtt_min,
                cast(avg(cast(rtt_mean as DECIMAL(16, 2))) as DECIMAL(16, 2))                      as avg_rtt_mean,
                percentile_approx(cast(rtt_std as DECIMAL(16, 2)), array(0.1, 0.3, 0.5, 0.7, 0.9)) as rtt_std_percentile,
                count(1) c1
         from (
                  select version,
                         if(local_proto is null,'forward',local_proto) local_proto,
                         case
                             when conn_type = '0' then 'erizo'
                             when conn_type = '1' then 'p2p'
                             when conn_type = '2' then 'sfu'
                             else 'other' end as type,
                         rtt_max,
                         rtt_min,
                         rtt_mean,
                         rtt_std
                  from ${APP}.dwd_sdk_du
                  where dt = '$do_date'
                    and version is not null
                    and over_num = success_num
                    and is_rtc = '1'
                    and cast(conn_time as BIGINT) >= 0
                    and cast(conn_time as BIGINT) < 172800
                    and cast(cap_fr_mean as BIGINT) < 200
                    and cast(rtt_max as BIGINT) < 20000
                    and cast(rtt_min as BIGINT) < 20000
                    and cast(rtt_mean as BIGINT) < 20000
              ) t1
         group by version, type ,local_proto grouping sets ((version, type ,local_proto),(version, local_proto),())
     ) t2;"

ads_rtc_version_loss="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_rtc_version_loss
select '$do_date',
       version,
       type,
       local_proto,
       avg_loss_max,
       avg_loss_mean,
       avg_loss_min,
       cast(loss_std_percentile[0] *100 as DECIMAL(16, 2)),
       cast(loss_std_percentile[1] *100 as DECIMAL(16, 2)),
       cast(loss_std_percentile[2] *100 as DECIMAL(16, 2)),
       cast(loss_std_percentile[3] *100 as DECIMAL(16, 2)),
       cast(loss_std_percentile[4] *100 as DECIMAL(16, 2)),
       c1
from (
         select if(t1.version is null,'total',t1.version) version,
                if(t1.type is null,'total',t1.type) type,
                if(t1.local_proto is null,'total',t1.local_proto) local_proto,
                cast(avg(loss_max) *100 as DECIMAL(16, 2))             as avg_loss_max,
                cast(avg(loss_mean) *100 as DECIMAL(16, 2))            as avg_loss_mean,
                cast(avg(loss_min) *100 as DECIMAL(16, 2))             as avg_loss_min,
                percentile_approx(cast(loss_std as DECIMAL(16, 4)),
                                  array(0.1, 0.3, 0.5, 0.7, 0.9)) as loss_std_percentile,
                count(1) c1
         from (
                  select version,
                         if(local_proto is null,'forward',local_proto) local_proto,
                         case
                             when conn_type = '0' then 'erizo'
                             when conn_type = '1' then 'p2p'
                             when conn_type = '2' then 'sfu'
                             else 'other' end as type,
                         loss_max,
                         loss_mean,
                         loss_min,
                         loss_std
                  from ${APP}.dwd_sdk_du
                  where dt = '$do_date'
                    and version is not null
                    and over_num = success_num
                    and is_rtc = '1'
                    and cast(conn_time as BIGINT) >= 0
                    and cast(conn_time as BIGINT) < 172800
                    and cast(cap_fr_mean as BIGINT) < 200
              ) t1
         group by version, type ,local_proto grouping sets ((version, type ,local_proto),(version, local_proto),())
     ) t2;"
ads_android_login_total="
insert overwrite table ${APP}.ads_android_login_total
select
    '$do_date',
    type,
    version,
    sum(if(action ='signin' AND result_code='1' AND (login_step = 'step2' or login_step is null ) ,1,0)) success_count,
    sum(if(action='signin_req' AND login_step='step2',1,0)) total_count,
    cast(sum(if(action ='signin' AND result_code='1' AND (login_step = 'step2' or login_step is null ),1,0))/sum(if(action='signin_req' AND login_step='step2',1,0)) * 100 AS DECIMAL(16,2))
from
(
    select
        case
       	    when platform='android' AND  get_json_object(ck_url,'$.auto_login')='1' AND get_json_object(ck_url,'$.login_type') ='0' then 'androidAccountAuto'
            when platform='android' AND  get_json_object(ck_url,'$.auto_login')='2' AND get_json_object(ck_url,'$.login_type') ='0' then 'androidAccountLogin'
            when platform='android' AND  get_json_object(ck_url,'$.auto_login')='1' AND get_json_object(ck_url,'$.login_type') ='1' then 'androidWechatAuto'
            when platform='android' AND  get_json_object(ck_url,'$.auto_login')='2' AND get_json_object(ck_url,'$.login_type') ='1' then 'androidWechatLogin'
            when platform='android' AND  get_json_object(ck_url,'$.auto_login')='1' AND get_json_object(ck_url,'$.login_type') ='2' then 'androidPhoneAuto'
            when platform='android' AND  get_json_object(ck_url,'$.auto_login')='2' AND get_json_object(ck_url,'$.login_type') ='2' then 'androidPhoneLogin'
	    when platform='iOS' AND  get_json_object(ck_url,'$.auto_login')='1' AND get_json_object(ck_url,'$.login_type')='0' then 'iOSAccountAutoLogin'
            when platform='iOS' AND  get_json_object(ck_url,'$.auto_login')='2' AND get_json_object(ck_url,'$.login_type')='0' then 'iOSAccountLogin'
	    when platform='iOS' AND  get_json_object(ck_url,'$.auto_login')='1' AND get_json_object(ck_url,'$.login_type') ='1' then 'iOSWechatAutoLogin'
            when platform='iOS' AND  get_json_object(ck_url,'$.auto_login')='2' AND get_json_object(ck_url,'$.login_type') ='1' then 'iOSWechatLogin'
	    when platform='iOS' AND  get_json_object(ck_url,'$.auto_login')='1' AND get_json_object(ck_url,'$.login_type') ='2' then 'iOSPhoneAutoLogin'
            when platform='iOS' AND  get_json_object(ck_url,'$.auto_login')='2' AND get_json_object(ck_url,'$.login_type') ='2' then 'iOSPhoneLogin'
            when platform='iOS' AND  get_json_object(ck_url,'$.auto_login')='1' AND get_json_object(ck_url,'$.login_type') ='4' then 'iOSAppleAutoLogin'
            when platform='iOS' AND  get_json_object(ck_url,'$.auto_login')='2' AND get_json_object(ck_url,'$.login_type') ='4' then 'iOSAppleLogin'
        else 'other' end type ,
        action,
        version,
        get_json_object(ck_url,'$.login_step') login_step,
        get_json_object(ck_url,'$.result_code') result_code
    from ${APP}.dwd_client_click
    where dt='$do_date'
    AND module='account'
    AND platform in('android','iOS')
    AND page='signin'
    AND version>='4.4.2'
    AND get_json_object(ck_url,'$.auto_login') is not null
    AND get_json_object(ck_url,'$.login_type') is not null
)t1
where type <> 'other'
group by type,version;"

ads_ios_login_total="
insert overwrite table ${APP}.ads_ios_login_total
select
    '$do_date',
    type,
    version,
    sum(if(action ='signin' AND result_code='1' AND login_step is null ,1,0)) success_count,
    sum(if(action='signin_req' AND login_step='step2',1,0)) total_count,
    cast(sum(if(action ='signin' AND result_code='1' AND login_step is null,1,0))/sum(if(action='signin_req' AND login_step='step2',1,0)) * 100 AS DECIMAL(16,2))
from
(
    select
        case
            when platform='iOS' AND  get_json_object(ck_url,'$.auto_login')='1' AND get_json_object(ck_url,'$.login_type') ='2' then 'iOSPhoneAutoLogin'
            when platform='iOS' AND  get_json_object(ck_url,'$.auto_login')='2' AND get_json_object(ck_url,'$.login_type') ='2' then 'iOSPhoneLogin'
            when platform='iOS' AND  get_json_object(ck_url,'$.auto_login')='1' AND get_json_object(ck_url,'$.login_type') ='4' then 'iOSAppleAutoLogin'
            when platform='iOS' AND  get_json_object(ck_url,'$.auto_login')='2' AND get_json_object(ck_url,'$.login_type') ='4' then 'iOSAppleLogin'
        else 'other' end type,
        action,
        version,
        get_json_object(ck_url,'$.login_step') login_step,
        get_json_object(ck_url,'$.result_code') result_code
    from ${APP}.dwd_client_click
    where dt='$do_date'
    AND module='account'
    AND platform = 'iOS'
    AND page='signin'
    AND version>='4.4.2'
    AND get_json_object(ck_url,'$.auto_login') is not null
    AND get_json_object(ck_url,'$.login_type') is not null
)t1
where type <> 'other'
group by type,version;"



ads_licode_session_day="
insert overwrite table ${APP}.ads_licode_session_day
select
    '$do_date',
    sum(connection_success_count),
    sum(connection_count),
    cast(sum(connection_success_count) / sum(connection_count) *100 as DECIMAL(16,2))
from ${APP}.dws_ip_licode_day_total
where dt='$do_date';"

ads_reconnect_session_day="
insert overwrite table ${APP}.ads_reconnect_session_day
select
    '$do_date',
    reconnect,
    count(1) new_connect_count,
    sum(if(handshake_success_connid is not null,1,0)) handshake_success_count,
    cast(sum(if(handshake_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) handshake_success_rate
from
(
    select
           connid,
           reconnect
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and reconnect is not null
)t1
left join
(
    select
        connid handshake_success_connid
    from ${APP}.dwd_handshake_success
    where dt='$do_date'
    group by connid
)t4
on t1.connid=t4.handshake_success_connid
group by reconnect;"

ads_client_licode_session_day="
insert overwrite table ${APP}.ads_client_licode_session_day
select
    dt,
    sclient,
    dclient,
    sversion,
    dversion,
    connection_success_count,
    connection_count
from ${APP}.dws_client_version_licode_day_total
where dt='$do_date'
and (sclient = 'ANDROID' or dclient = 'ANDROID');"

ads_web_setting_total="
insert overwrite table ${APP}.ads_web_setting_total
select
    '$do_date',
    cast(sum(if(action='resp',1,0))/sum(if(action='req',1,0)) *100 AS DECIMAL(16,2))
from ${APP}.dwd_web_config_center
where dt='$do_date'
and (action='req' or action='resp');"

ads_su_province_total="
insert overwrite table ${APP}.ads_su_province_total
select
    '$do_date',
    province_name,
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
        sprovince,
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
               else 'other' end customer_type
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
    group by sprovince,customer_type
) su_session_rate
on area.province_name=su_session_rate.sprovince;"

ads_android_licode_total="
insert overwrite table ${APP}.ads_android_licode_total
select
    '$do_date',
    version,
    \`turndata_useLicode\`,
    turndata_licode,
    count(distinct(turndata_connid))
from
(
    select
        \`turndata_useLicode\`,
        case when \`turndata_useLicode\` = 'false' and turndata_process = '5' then '连接服务器'
             when \`turndata_useLicode\` = 'false' and turndata_process = '10' then '登录服务器'
             when \`turndata_useLicode\` = 'false' and turndata_process = '20' then '连接中转服务器'
             when \`turndata_useLicode\` = 'false' and turndata_process = '75' then '登录服务器'
             when \`turndata_useLicode\` = 'false' and turndata_process = '100' then '加载完成'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '5' then '连接服务器'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '10' then '登录服务器'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '20' then '连接中转服务器'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '30' then '连接中转服务器'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '40' then '连接rtc媒体服务'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '50' then '登录rtc媒体服务'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '60' then '连接p2p服务'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '70' then '连接p2p服务成功'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '100' then '加载完成'
        else '-' end turndata_licode,
        turndata_connid,
        version
    from ${APP}.dwd_client_tuevent
    where dt='$do_date'
    and version>='4.4.3'
    and platform='android'
    and action='remote_process'
    and events='remote_state_process'
)t1
where turndata_licode <> '-'
group by version,\`turndata_useLicode\`,turndata_licode;"

ads_ios_licode_total="
insert overwrite table ${APP}.ads_ios_licode_total
select
    '$do_date',
    version,
    \`turndata_useLicode\`,
    turndata_licode,
    count(distinct(turndata_connid))
from
(
    select
        \`turndata_useLicode\`,
        case
             when \`turndata_useLicode\` = 'false' and turndata_process = '0' then '开始连接'
             when \`turndata_useLicode\` = 'false' and turndata_process = '5' then '连接服务器'
             when \`turndata_useLicode\` = 'false' and turndata_process = '10' then '登录服务器'
             when \`turndata_useLicode\` = 'false' and turndata_process = '20' then '连接中转服务器'
             when \`turndata_useLicode\` = 'false' and turndata_process = '75' then '登录服务器'
             when \`turndata_useLicode\` = 'false' and turndata_process = '100' then '加载完成'
             when \`turndata_useLicode\` = 'true' and turndata_process = '0' then '开始连接'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '5' then '连接服务器'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '10' then '登录服务器'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '20' then '连接中转服务器'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '30' then '连接中转服务器'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '40' then '连接rtc媒体服务'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '50' then '登录rtc媒体服务'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '60' then '连接p2p服务'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '70' then '连接p2p服务成功'
             when \`turndata_useLicode\` = 'true'  and turndata_process = '100' then '加载完成'
        else '-' end turndata_licode,
        turndata_connid,
        version
    from ${APP}.dwd_client_tuevent
    where dt='$do_date'
    and version>='4.4.3'
    and platform='iOS'
    and action='remote_process'
    and events='remote_state_process'
)t1
where turndata_licode <> '-'
group by version,\`turndata_useLicode\`,turndata_licode;"

ads_licode_sdk_total="
insert overwrite table ${APP}.ads_licode_sdk_total
select
    '$do_date',
    version,
    sum(cast(turndata_alltime/1000 as BIGINT))
from ${APP}.dwd_licode_sdk
where dt='$do_date'
and version is not null 
and cast(turndata_alltime as bigint) < 2592000000
group by version;"

ads_licode_sig_total="
insert overwrite table ${APP}.ads_licode_sig_total
select
    '$do_date',
    turndata_version,
    turndata_url,
    sum(if(turndata_connected='0',1,0)), 
    sum(if(turndata_connected='1',1,0)), 
    count(1) c1,
    cast(sum(if(turndata_connected='1',1,0))/count(1) *100 as DECIMAL(16,2)),
sum(cast(turndata_disconnectiontimes as BIGINT)),
cast(sum(cast(turndata_disconnectiontimes as BIGINT)) /count(1) as DECIMAL(16,2)),
sum(if(turndata_connected='1' and cast(turndata_connectiontime as bigint) <=10000,cast(turndata_connectiontime/1000 as DECIMAL(16,4)),0)),
sum(if(turndata_connected='1',cast(turndata_offlinetime/1000 as DECIMAL(16,4)),0)),
sum(if(turndata_connected='1',cast(turndata_onlinetime/1000 as DECIMAL(16,4)),0)),
sum(if(turndata_connected='1' and cast(turndata_connectiontime as bigint) <=10000,cast(turndata_connectiontime/1000 as DECIMAL(16,4)),0))+sum(if(turndata_connected='1',cast(turndata_offlinetime/1000 as DECIMAL(16,4)),0))+sum(if(turndata_connected='1',cast(turndata_onlinetime/1000 as DECIMAL(16,4)),0)),
cast(sum(if(turndata_connected='1' and cast(turndata_connectiontime as bigint) <=10000,cast(turndata_connectiontime/1000 as DECIMAL(16,4)),0)) / sum(if(turndata_connected='1',1,0))  as DECIMAL(16,4)),
cast(sum(if(turndata_connected='1',cast(turndata_offlinetime/1000 as DECIMAL(16,4)),0)) / sum(if(turndata_connected='1',1,0))  as DECIMAL(16,4)),
cast(sum(if(turndata_connected='1',cast(turndata_onlinetime/1000 as DECIMAL(16,4)),0)) / sum(if(turndata_connected='1',1,0))  as DECIMAL(16,4)),
cast((sum(if(turndata_connected='1' and cast(turndata_connectiontime as bigint) <=10000,cast(turndata_connectiontime/1000 as DECIMAL(16,4)),0))+sum(if(turndata_connected='1',cast(turndata_offlinetime/1000 as DECIMAL(16,4)),0))+sum(if(turndata_connected='1',cast(turndata_onlinetime/1000 as DECIMAL(16,4)),0)))/ sum(if(turndata_connected='1',1,0))  as DECIMAL(16,4))
from
(
    select
        turndata_sdk_id,
        turndata_connected,
        turndata_url,
        turndata_disconnectiontimes,
        turndata_connectiontime,
        turndata_offlinetime,
        turndata_onlinetime
    from ${APP}.dwd_licode_sig
    where dt='$do_date'
    and cast(turndata_connectiontime as BIGINT) < 2592000000
    and cast(turndata_offlinetime as BIGINT) < 2592000000
    and cast(turndata_onlinetime as BIGINT) < 2592000000
)t1
left join
(
    select
        turndata_id,
        turndata_version
    from ${APP}.dwd_licode_sdk
    where dt='$do_date'
    and cast(turndata_alltime as bigint) < 2592000000
)t2
on turndata_sdk_id=turndata_id
where turndata_version is not null
and turndata_url is not null
group by turndata_version,turndata_url
having c1 > 200
order by c1;"

ads_licode_ice_total="
insert overwrite table ${APP}.ads_licode_ice_total
select
    '$do_date',
    turndata_version,
    turndata_connection_type,
    sum(if(turndata_connected='0',1,0)), 
    sum(if(turndata_connected='1',1,0)), 
    count(1) c1,
    cast(sum(if(turndata_connected='1',1,0))/sum(if(turndata_connected='0' or turndata_connected='1',1,0)) *100 as DECIMAL(16,2)),
    sum(if(turndata_activeconnection='1',1,0)),
    sum(if(turndata_activeconnection='1',cast(turndata_onlinetime/1000 as DECIMAL(16,4)),0)),
sum(cast(turndata_disconnectiontimes as BIGINT)),
cast(sum(cast(turndata_disconnectiontimes as BIGINT)) /count(1) as DECIMAL(16,2)),
sum(if(turndata_connected='1' and cast(turndata_connectiontime as bigint) <=10000,cast(turndata_connectiontime/1000 as DECIMAL(16,4)),0)),
sum(if(turndata_connected='1',cast(turndata_offlinetime/1000 as DECIMAL(16,4)),0)),
sum(if(turndata_connected='1',cast(turndata_onlinetime/1000 as DECIMAL(16,4)),0)),
sum(if(turndata_connected='1' and cast(turndata_connectiontime as bigint) <=10000,cast(turndata_connectiontime/1000 as DECIMAL(16,4)),0))+sum(if(turndata_connected='1',cast(turndata_offlinetime/1000 as DECIMAL(16,4)),0))+sum(if(turndata_connected='1',cast(turndata_onlinetime/1000 as DECIMAL(16,4)),0)),
cast(sum(if(turndata_connected='1' and cast(turndata_connectiontime as bigint) <=10000,cast(turndata_connectiontime/1000 as DECIMAL(16,4)),0)) / sum(if(turndata_connected='1',1,0))  as DECIMAL(16,4)),
cast(sum(if(turndata_connected='1',cast(turndata_offlinetime/1000 as DECIMAL(16,4)),0)) / sum(if(turndata_connected='1',1,0))  as DECIMAL(16,4)),
cast(sum(if(turndata_connected='1',cast(turndata_onlinetime/1000 as DECIMAL(16,4)),0)) / sum(if(turndata_connected='1',1,0))  as DECIMAL(16,4)),
cast((sum(if(turndata_connected='1',cast(turndata_connectiontime/1000 as DECIMAL(16,4)),0))+sum(if(turndata_connected='1',cast(turndata_offlinetime/1000 as DECIMAL(16,4)),0))+sum(if(turndata_connected='1',cast(turndata_onlinetime/1000 as DECIMAL(16,4)),0)))/ sum(if(turndata_connected='1',1,0))  as DECIMAL(16,4)),
sum(if((bytes_received is not null and  cast(bytes_received as bigint) <> 0) or (bytes_sent is not null and  cast(bytes_sent as bigint) <> 0),1,0 )),
    sum(if((bytes_received is not null and  cast(bytes_received as bigint) <> 0),1,0 )),
    sum(if((bytes_sent is not null and  cast(bytes_sent as bigint) <> 0),1,0 )),
    sum(if((track_audio = 0),1,0 )),
    sum(if((track_audio = 1),1,0 )),
    sum(if((track_audio = 2),1,0 )),
    sum(if((track_video = 0),1,0 )),
    sum(if((track_video = 1),1,0 )),
    sum(if((track_video = 2),1,0 ))
from
(
    select
        turndata_sdk_id,
	turndata_activeconnection,
        turndata_connected,
        turndata_connection_type,
        turndata_disconnectiontimes,
        turndata_connectiontime,
        turndata_offlinetime,
        turndata_onlinetime,
        bytes_received,
        bytes_sent,
        track_audio,
        track_video
    from ${APP}.dwd_licode_ice
    where dt='$do_date'
    and cast(turndata_connectiontime as BIGINT) < 2592000000
    and cast(turndata_offlinetime as BIGINT) < 2592000000
    and cast(turndata_onlinetime as BIGINT) < 2592000000
)t1
left join
(
    select
        turndata_id,
        turndata_version
    from ${APP}.dwd_licode_sdk
    where dt='$do_date'
    and cast(turndata_alltime as bigint) < 2592000000
)t2
on turndata_sdk_id=turndata_id
where turndata_version is not null
and turndata_connection_type is not null
group by turndata_version,turndata_connection_type
having c1 > 500
order by c1;"


ads_windows_first_frame_fail="
insert overwrite table ${APP}.ads_windows_first_frame_fail
select
    '$do_date',
    t1.version,
    t1.type,
    t1.err,
    t1.c1 + if(t2.c1 is not null ,t2.c1,0)
from
(
    select
        get_json_object(errno,'$.link') type,
        get_json_object(errno,'$.rollback') rol,
        get_json_object(errno,'$.errno') err,
        version version,
        count(1) c1
    from ${APP}.dwd_client_tevent
    where dt='$do_date'
    and action='connectFailReason'
    and platform='windows'
    and version >= '4.3.0'
    and get_json_object(errno,'$.rollback')='0'
    group by get_json_object(errno,'$.link'),get_json_object(errno,'$.rollback'),get_json_object(errno,'$.errno'),version
)t1
left join
(
    select
        get_json_object(errno,'$.link') type,
        get_json_object(errno,'$.rollback') rol,
        get_json_object(errno,'$.errno') err,
        version version,
        count(1) c1
    from ${APP}.dwd_client_tevent
    where dt='$do_date'
    and action='connectFailReason'
    and platform='windows'
    and version >= '4.3.0'
    and get_json_object(errno,'$.link')='licode'
    and get_json_object(errno,'$.rollback')='1'
    group by get_json_object(errno,'$.link'),get_json_object(errno,'$.rollback'),get_json_object(errno,'$.errno'),version
)t2
on t1.version=t2.version AND t1.err=t2.err;"

ads_windows_first_frame_success="
insert overwrite table ${APP}.ads_windows_first_frame_success
select
    '$do_date',
    version,
    type,
    ts,
    c1,
    cast(c1 / sum(c1) over(partition by version,type) *100 as DECIMAL(16,2))
from
(
    select
        version,
        type,
        ts,
        count(1) c1
    from
    (
        select
            version,
            case
                when get_json_object(errno,'$.link')='forward' AND get_json_object(errno,'$.rollback')='0' then 'forward'
                when get_json_object(errno,'$.link')='licode' AND get_json_object(errno,'$.rollback')='1' then 'forward'
                when get_json_object(errno,'$.link')='licode' AND get_json_object(errno,'$.rollback')='0' then 'licode'
            else '-' end type,
            case
                when cast(get_json_object(errno,'$.time') as BIGINT) <= 200 then '200-'
                when cast(get_json_object(errno,'$.time') as BIGINT) > 200 and cast(get_json_object(errno,'$.time') as BIGINT) <= 500 then '500-'
                when cast(get_json_object(errno,'$.time') as BIGINT) > 500 and cast(get_json_object(errno,'$.time') as BIGINT) <= 1000 then '1000-'
                when cast(get_json_object(errno,'$.time') as BIGINT) > 1000 then '1000+'
            else '-' end ts
        from ${APP}.dwd_client_tevent
        where dt='$do_date'
        and action='connectDuration'
        and platform='windows'
        and version >= '4.3.0'
    )t1
    group by version,type,ts
)t2;"


ads_licode_conn_time_total="
insert overwrite table ${APP}.ads_licode_conn_time_total
select
    '$do_date',
    if(labtype is null,'other',labtype),if(biztype is null,'other',biztype),if(is_file_transport is null,'other',is_file_transport),
    sum(link_time),
    count(distinct connid),
    cast(sum(link_time)/count(1) as bigint)
from ${APP}.dwd_licode_session_success
where dt='$do_date'
group by labtype,biztype,is_file_transport;"

ads_version_new_connect_total="
insert overwrite table ${APP}.ads_version_new_connect_total
select
    '$do_date',
    'todesk',
    sclient,
    sversion,
    count(1) c1,0
from
(
    select
        suid,  
        sversion,
        sclient
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and sclient in ('PC','MAC')
    and labtype='excalibur'
    and enterprise='0'
    union 
    select
        duid,
        dversion,
        dclient
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and dclient in ('PC','MAC')
    and labtype='excalibur'
    and enterprise='0'
)t1
group by sclient,sversion
union
select
    '$do_date',
    'daas',
    sclient,
    sversion,
    count(1) c1,0
from
(
    select
        suid,  
        sversion,
        if(sclient='1','PC','MAC') sclient
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and sclient in ('1','2')
    and labtype='daas'
    union 
    select
        duid,
        dversion,
        if(dclient='1','PC','MAC')  dclient
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and dclient in ('1','2')
    and labtype='daas'
)t1
group by sclient,sversion;"

ads_version_new_connect_clientype_total="
insert overwrite table ${APP}.ads_version_new_connect_clientype_total
select
    '$do_date',clientype,sversion,c1,cast(c1/sum(c1) over(partition by clientype) *100 as DECIMAL(16,2))
from
(
    select
        clientype,sversion,count(1) c1 
    from
    (
        select
            '0' clientype,
            sversion,
            suid
            from ${APP}.dwd_new_connect
            where dt='$do_date'
            and sclient='PC'
            group by sversion,suid  
        union all
        select
            '1',
            dversion,
            duid
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        and dclient='PC'
        group by dversion,duid
    )t1
    group by clientype, sversion
)t2;"

ads_backend_licode_loss_total="
insert overwrite table ${APP}.ads_backend_licode_loss_total
select
    '$do_date',
    type,
    metadata_type,
    mx,
    mn,
    ag,
    cast(pct[0] as DECIMAL(16,2)),
    cast(pct[1] as DECIMAL(16,2)),
    cast(pct[2] as DECIMAL(16,2)),
    cast(pct[3] as DECIMAL(16,2)),
    cast(pct[4] as DECIMAL(16,2))
from
(
    select
        'loss_packet_rate' as type,
        metadata_type,
        max(loss_packet_rate) mx,
        min(loss_packet_rate) mn,
        cast(avg(loss_packet_rate) as decimal(16,2))  ag,
        percentile_approx(loss_packet_rate, array(0.1,0.3,0.5,0.75,0.95)) pct
    from ${APP}.dwd_media_statistic
    where dt='$do_date'
    and loss_packet_rate >= 0
    and metadata_type is not null
    group by metadata_type
    union all
     select
        'delay0_10_ms' as type,
        metadata_type,
        max(cast(delay0_10_ms as BIGINT)) mx,
        min(cast(delay0_10_ms as BIGINT)) mn,
        cast(avg(cast(delay0_10_ms as BIGINT)) as decimal(16,2))  ag,
        percentile_approx(cast(delay0_10_ms as DECIMAL(16,2)), array(0.1,0.3,0.5,0.75,0.95)) pct
    from ${APP}.dwd_media_statistic
    where dt='$do_date'
    and cast(delay0_10_ms as bigint ) >0
    and metadata_type is not null
    group by metadata_type
    union all
    select
        'delay10_50_ms',
        metadata_type,
        max(cast(delay10_50_ms as BIGINT)),
        min(cast(delay10_50_ms as BIGINT)),
        cast(avg(cast(delay10_50_ms as BIGINT)) as decimal(16,2)),
        percentile_approx(cast(delay10_50_ms as DECIMAL(16,2)), array(0.1,0.3,0.5,0.75,0.95)) as loss_std_percentile
    from ${APP}.dwd_media_statistic
    where dt='$do_date'
    and cast(delay10_50_ms as bigint ) >0
    and metadata_type is not null
    group by metadata_type
    union all
     select
        'delay50_100_ms' as type,
        metadata_type,
        max(cast(delay50_100_ms as BIGINT)) mx,
        min(cast(delay50_100_ms as BIGINT)) mn,
        cast(avg(cast(delay50_100_ms as BIGINT)) as decimal(16,2))  ag,
        percentile_approx(cast(delay50_100_ms as DECIMAL(16,2)), array(0.1,0.3,0.5,0.75,0.95)) pct
    from ${APP}.dwd_media_statistic
    where dt='$do_date'
    and cast(delay50_100_ms as bigint ) >0
    and metadata_type is not null
    group by metadata_type
    union all
    select
        'delay100_1000_ms',
        metadata_type,
        max(cast(delay100_1000_ms as BIGINT)),
        min(cast(delay100_1000_ms as BIGINT)),
        cast(avg(cast(delay100_1000_ms as BIGINT)) as decimal(16,2)),
        percentile_approx(cast(delay100_1000_ms as DECIMAL(16,2)), array(0.1,0.3,0.5,0.75,0.95)) as loss_std_percentile
    from ${APP}.dwd_media_statistic
    where dt='$do_date'
    and cast(delay100_1000_ms as bigint ) >0
    and metadata_type is not null
    group by metadata_type
    union all
    select
        'delay1000_ms',
        metadata_type,
        max(cast(delay1000_ms as BIGINT)),
        min(cast(delay1000_ms as BIGINT)),
        cast(avg(cast(delay1000_ms as BIGINT)) as decimal(16,2)),
        percentile_approx(cast(delay1000_ms as DECIMAL(16,2)), array(0.1,0.3,0.5,0.75,0.95)) as loss_std_percentile
    from ${APP}.dwd_media_statistic
    where dt='$do_date'
    and cast(delay1000_ms as bigint ) >0
    and metadata_type is not null
    group by metadata_type
    union all
     select
        'duration0_10_ms' as type,
        metadata_type,
        max(cast(duration0_10_ms as BIGINT)) mx,
        min(cast(duration0_10_ms as BIGINT)) mn,
        cast(avg(cast(duration0_10_ms as BIGINT)) as decimal(16,2))  ag,
        percentile_approx(cast(duration0_10_ms as DECIMAL(16,2)), array(0.1,0.3,0.5,0.75,0.95)) pct
    from ${APP}.dwd_media_statistic
    where dt='$do_date'
    and cast(duration0_10_ms as bigint ) >0
    and metadata_type is not null
    group by metadata_type
    union all
    select
        'duration10_50_ms',
        metadata_type,
        max(cast(duration10_50_ms as BIGINT)),
        min(cast(duration10_50_ms as BIGINT)),
        cast(avg(cast(duration10_50_ms as BIGINT)) as decimal(16,2)),
        percentile_approx(cast(duration10_50_ms as DECIMAL(16,2)), array(0.1,0.3,0.5,0.75,0.95)) as loss_std_percentile
    from ${APP}.dwd_media_statistic
    where dt='$do_date'
    and cast(duration10_50_ms as bigint ) >0
    and metadata_type is not null
    group by metadata_type
    union all
     select
        'duration50_100_ms' as type,
        metadata_type,
        max(cast(duration50_100_ms as BIGINT)) mx,
        min(cast(duration50_100_ms as BIGINT)) mn,
        cast(avg(cast(duration50_100_ms as BIGINT)) as decimal(16,2))  ag,
        percentile_approx(cast(duration50_100_ms as DECIMAL(16,2)), array(0.1,0.3,0.5,0.75,0.95)) pct
    from ${APP}.dwd_media_statistic
    where dt='$do_date'
    and cast(duration50_100_ms as bigint ) >0
    and metadata_type is not null
    group by metadata_type
    union all
    select
        'delay100_1000_ms',
        metadata_type,
        max(cast(delay100_1000_ms as BIGINT)),
        min(cast(delay100_1000_ms as BIGINT)),
        cast(avg(cast(delay100_1000_ms as BIGINT)) as decimal(16,2)),
        percentile_approx(cast(delay100_1000_ms as DECIMAL(16,2)), array(0.1,0.3,0.5,0.75,0.95)) as loss_std_percentile
    from ${APP}.dwd_media_statistic
    where dt='$do_date'
    and cast(delay100_1000_ms as bigint ) >0
    and metadata_type is not null
    group by metadata_type
    union all
    select
        'duration1000ms',
        metadata_type,
        max(cast(duration1000ms as BIGINT)),
        min(cast(duration1000ms as BIGINT)),
        cast(avg(cast(duration1000ms as BIGINT)) as decimal(16,2)),
        percentile_approx(cast(duration1000ms as DECIMAL(16,2)), array(0.1,0.3,0.5,0.75,0.95)) as loss_std_percentile
    from ${APP}.dwd_media_statistic
    where dt='$do_date'
    and cast(duration1000ms as bigint ) >0
    and metadata_type is not null
    group by metadata_type
    union all
    select
        if(flag_ms is null,'all',flag_ms),
        'none',
        max(cast(first_packet_ms as bigint)),
        min(cast(first_packet_ms as bigint)),
        cast(avg(cast(first_packet_ms as BIGINT)) as decimal(16,2)),
        percentile_approx(cast(first_packet_ms as DECIMAL(16,2)), array(0.1,0.3,0.5,0.75,0.95)) as loss_std_percentile
    from
     (
        select
            first_packet_ms ,
            case
                when cast(first_packet_ms as bigint) between 0 and 10 then 'first_packet0_10_ms'
                when cast(first_packet_ms as bigint) between 10 and 50 then 'first_packet10_50_ms'
                when cast(first_packet_ms as bigint) between 50 and 100 then 'first_packet50_100_ms'
                when cast(first_packet_ms as bigint) between 100 and 1000 then 'first_packet100_1000_ms'
                when cast(first_packet_ms as bigint) > 1000 then 'first_packet_receive1000ms'
            else 'other' end flag_ms,'6' type
        from ${APP}.dwd_licode_first_packet_receive
        where dt='$do_date'
        and cast(first_packet_ms as bigint ) >0
     )tt
    group by flag_ms,type grouping sets (flag_ms,type)
)t1;"


ads_windows_clarity_total="
insert overwrite table ${APP}.ads_windows_clarity_total
select
    '$do_date',
    get_json_object(ck_url,'$.clarity'),
    count(1)
from ${APP}.dwd_client_event
where dt='$do_date'
and module='connect'
and action='clarity'
and get_json_object(ck_url,'$.clarity') is not null
group by get_json_object(ck_url,'$.clarity');"

ads_id_request_phone_total="
insert overwrite table ${APP}.ads_id_request_phone_total
select
    '$do_date',
    'login',
    count(distinct(id))
from ${APP}.dwd_web_request
where dt='$do_date'
and ck_url='/api/phone/verifyPhoneCode'
union all
select
    '$do_date',
       'login_err',
       count(distinct(id))
from ${APP}.dwd_login_error
where dt='$do_date'
and ck_url='/api/phone/verifyPhoneCode';"

ads_version_new_connect_type_total="
insert overwrite table ${APP}.ads_version_new_connect_type_total
select
    '$do_date',dclient,dversion,count(1)
from
(
    select
        dclient,
        dversion,
        duid,
        count(1) c1
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and dclient in ('PC','ONTHER')
    group by dclient,dversion,duid
)t1
group by dclient, dversion;"



ads_android_logic_topic_total="
insert overwrite table ${APP}.ads_android_logic_topic_total
select
    '$do_date',
    client_type,
    bus,
    sum(if(first_logic_date='$do_date',1,0)),
    sum(if(last_logic_date='$do_date',1,0))
from ${APP}.dwt_android_topic
where dt = '$do_date'
and client_type is not null
group by client_type,bus;"

ads_android_logic_retention="
insert overwrite table ${APP}.ads_android_logic_retention
select * from ${APP}.ads_android_logic_retention
union
select *
from
(
    select
        '$do_date',
        first_logic_date d1,
        client_type,
        bus,
        datediff('$do_date',first_logic_date) c1, 
        sum(if(last_logic_date='$do_date',1,0)),
        count(*) 
    from ${APP}.dwt_android_topic
    where dt='$do_date'
    and first_logic_date>=date_add('$do_date',-30)
    and first_logic_date<'$do_date'
    group by first_logic_date,client_type,bus  
)t1
where t1.c1 in(1,2,3,4,5,6,7,14,30);"

ads_connection_total="
set hive.support.quoted.identifiers=None;
insert overwrite table ${APP}.ads_connection_total
select
    '$do_date',\`(r1)?+.+\`
from
(
    select
    *,
    rank() over (partition by enterprise order by new_connect_count desc) r1
from
(
    select
    t7.sversion,
    t7.dversion,
    enterprise,
    sum(if(new_connect_id is not null,1,0)) new_connect_count,
    sum(if(handshake_success_connid is not null,1,0)) handshake_success_count,
    sum(if(authentication_success_id is not null,1,0)) authentication_success_count,
    sum(if(connstart_connid is not null and authentication_success_id is not null ,1,0)) connstart_count,
    sum(if(authentication_success_id is not null and conn_mode='licode',1,0)) start_licode_count,
    sum(if(authentication_success_id is not null and conn_mode='forward',1,0)) start_forward_count,
    sum(if(connstart_connid is not null and authentication_success_id is not null and link_info_report_id is not null,1,0)) link_info_report_count,
    sum(if(connstart_connid is not null and authentication_success_id is not null and link_info_report_id is not null and conn_mode='licode',1,0)) start_licode_has_report_count,
    sum(if(connstart_connid is not null and authentication_success_id is not null and link_info_report_id is not null and conn_mode='licode' and rtc_id is not null,1,0)) start_licode_has_rtc_reoprt_count,
    sum(if(connstart_connid is not null and authentication_success_id is not null and link_info_report_id is not null and conn_mode='licode' and rtc_id is null,1,0))  start_licode_hasnt_rtc_report_count,
    sum(if(connstart_connid is not null and authentication_success_id is not null and link_info_report_id is not null and conn_mode='forward' and rtc_id is null,1,0)) start_forward_direct_count1,
    sum(if(connstart_connid is not null and authentication_success_id is not null and link_info_report_id is not null and conn_mode='forward' and rtc_id is not null,1,0)) start_forward_direct_count2,
    sum(if(connstart_connid is not null and authentication_success_id is not null and link_info_report_id is not null and conn_mode='licode' and has_video_id is not null,1,0)) rtc_report_has_video_count,
    sum(if(connstart_connid is not null and authentication_success_id is not null and link_info_report_id is not null and conn_mode='licode' and rtc_id is null
        and forward_link_info_id is not null and forwardFirstFrameTime > 0,1,0)) miss_rtc_report_has_video_count,
    sum(if(connstart_connid is not null and authentication_success_id is not null and link_info_report_id is not null and conn_mode='forward' and rtc_id is null
        and forward_link_info_id is not null and forwardFirstFrameTime > 0,1,0)) forward_has_video_count,
    sum(if(rtc_id is not null,1,0)) rtc_count0,
    sum(if(has_video_id is not null,1,0)) has_video_count0
from (
    select
        t5.sversion sversion,
        t5.dversion dversion,enterprise,
        new_connect_id,
        handshake_success_connid,
        authentication_success_id,
        connstart_connid, conn_mode,
        link_info_report_id,
        rtc_id,
        has_video_id
    from (
    select
        t3.sversion sversion,
        t3.dversion dversion,enterprise,
        new_connect_id,
        handshake_success_connid,
        authentication_success_id,
        connstart_connid, conn_mode,
        link_info_report_id,
        rtc_id
        from (
            select
            t1.sversion sversion,
            t1.dversion dversion,enterprise,
            new_connect_id,
            handshake_success_connid,
            authentication_success_id,
            connstart_connid, conn_mode,
            link_info_report_id
            from (
                select  t04.sversion sversion, t04.dversion dversion,enterprise,
                  new_connect_id,
                  handshake_success_connid,
                  authentication_success_id,
                  connstart_connid, conn_mode
                   from(
                    select  t02.sversion sversion, t02.dversion dversion,enterprise,
                    new_connect_id,
                    handshake_success_connid,
                    authentication_success_id
                    from(
                            select  t00.sversion sversion, t00.dversion dversion,enterprise,
                            handshake_success_connid,
                            new_connect_id
                        from (
                            select connid new_connect_id, sversion, dversion,enterprise    
                            from ${APP}.dwd_new_connect
                            where dt='$do_date'
                            and sclient in ('PC','1')
                            and dclient in ('PC','1')
                        )t00 left join (                                            
                            select connid handshake_success_connid
                            from ${APP}.dwd_handshake_success
                            where dt='$do_date'
                            group by connid
                        )t01 on t00.new_connect_id = handshake_success_connid       
                    ) t02 left join(
                        SELECT dt,                                                  
                            connid authentication_success_id
                        FROM ${APP}.dwd_authentication_success
                        WHERE dt = '$do_date'
                        group by dt, connid
                    ) t03 on t02.new_connect_id = authentication_success_id         
                   ) t04 left join(
                        select connstart_connid, conn_mode
                        from (
                        select connstart_connid, conn_mode, rank() over (partition by connstart_connid order by conn_mode desc) as rn
                        from(select conn_id connstart_connid,
                            get_json_object(errno, '$.mode') as conn_mode
                            from ${APP}.dwd_client_tevent
                            where dt='$do_date' and eventtype = 'vcli' and action='connstart'
                            group by conn_id, get_json_object(errno, '$.mode'))t1
                        )s00 where s00.rn = 1
                   ) t05 on t04.new_connect_id = connstart_connid
            ) t1 left join (
                select
                    conn_id link_info_report_id
                from ${APP}.dwd_client_tuevent                           
                where dt = '$do_date' and clientype='client' and events='link_info'
                group by  conn_id
        ) t2 on t1.new_connect_id = t2.link_info_report_id                  
    ) t3 left join (
        SELECT
            conn_id rtc_id
        FROM ${APP}.dwd_sdk_su                                   
        WHERE dt = '$do_date'
        group by conn_id                                   
    ) t4 on t3.new_connect_id = t4.rtc_id
) t5 left join (
    select
        conn_id has_video_id
    from ${APP}.dwd_sdk_su
    where dt='$do_date' and over_num=success_num and cast(first_frame_time as bigint) > 0
    group by  conn_id
) t6 on t5.new_connect_id = t6.has_video_id                         
) t7 left join (
    select  conn_id forward_link_info_id, cast(get_json_object(turndata, '$.FirstFrameTime') as bigint) as forwardFirstFrameTime
        from ${APP}.dwd_link_info
        where dt='$do_date' and clientype='client' and turndata not like  '[%'
) t8 on t7.new_connect_id = t8.forward_link_info_id
group by  t7.sversion,t7.dversion,enterprise
)tt
)t1
where r1<=10;"

ads_p2p_network_total="
insert overwrite table ${APP}.ads_p2p_network_total
select '$do_date',
       client_network,
       host_network,
       client_version,
       host_version,
       cast(avg(rtt_mean)                                                                   as DECIMAL(16,4)) as rtt_mean,
       cast(avg(loss_mean)                                                                  as DECIMAL(16,4)) as loss_mean,
       cast(avg(rl_loss_mean)                                                               as DECIMAL(16,4)) as rl_loss_mean,
       cast(avg(rl_loss_max)                                                                as DECIMAL(16,4)) as rl_loss_max,
       cast(avg(dec_stuck_time) / avg(conn_time * 10)                                       as DECIMAL(16,4)) as stuck_rate,
       cast(avg(first_frame_time)                                                           as DECIMAL(16,4)) as avg_first_frame_time,
       cast(percentile_approx(cast(rtt_mean as bigint), 0.1)                                as DECIMAL(16,4)) as rttP10,
       cast(percentile_approx(cast(rtt_mean as bigint), 0.3)                                as DECIMAL(16,4)) as rttP30,
       cast(percentile_approx(cast(rtt_mean as bigint), 0.5)                                as DECIMAL(16,4)) as rttP50,
       cast(percentile_approx(cast(rtt_mean as bigint), 0.7)                                as DECIMAL(16,4)) as rttP70,
       cast(percentile_approx(cast(rtt_mean as bigint), 0.9)                                as DECIMAL(16,4)) as rttP90,
       cast(percentile_approx(cast(loss_mean as bigint), 0.1)                               as DECIMAL(16,4)) as lossP10,
       cast(percentile_approx(cast(loss_mean as bigint), 0.3)                               as DECIMAL(16,4)) as lossP30,
       cast(percentile_approx(cast(loss_mean as bigint), 0.5)                               as DECIMAL(16,4)) as lossP50,
       cast(percentile_approx(cast(loss_mean as bigint), 0.7)                               as DECIMAL(16,4)) as lossP70,
       cast(percentile_approx(cast(loss_mean as bigint), 0.9)                               as DECIMAL(16,4)) as lossP90,
       cast(percentile_approx(cast(dec_stuck_time / conn_time / 10 as DECIMAL(16, 2)), 0.1) as DECIMAL(16,4)) as stuckTP10,
       cast(percentile_approx(cast(dec_stuck_time / conn_time / 10 as DECIMAL(16, 2)), 0.3) as DECIMAL(16,4)) as stuckTP30,
       cast(percentile_approx(cast(dec_stuck_time / conn_time / 10 as DECIMAL(16, 2)), 0.5) as DECIMAL(16,4)) as stuckTP50,
       cast(percentile_approx(cast(dec_stuck_time / conn_time / 10 as DECIMAL(16, 2)), 0.7) as DECIMAL(16,4)) as stuckTP70,
       cast(percentile_approx(cast(dec_stuck_time / conn_time / 10 as DECIMAL(16, 2)), 0.9) as DECIMAL(16,4)) as stuckTP90,
       count(*)                                                                        as count
from (
         select host.conn_id,
                host.network   as host_network,
                client.network as client_network,
                host.version   as host_version,
                client.version as client_version,
                host.rtt_mean,
                host.loss_mean,
                host.rl_loss_mean,
                host.rl_loss_max,
                client.first_frame_time,
                client.dec_stuck_time,
                client.conn_time
         from (
                  select case
                             when network like '%移动' then '移动'
                             when network like '%联通' then '联通'
                             when network like '%电信' then '电信'
                             else '其他' end as                 network,
                         version                                version,
                         conn_id,
                         cast(rtt_mean as DECIMAL)            rtt_mean,
                         cast(loss_mean as DECIMAL(16, 3))    loss_mean,
                         cast(rl_loss_mean as DECIMAL(16, 3)) rl_loss_mean,
                         cast(rl_loss_max as DECIMAL(16, 3))  rl_loss_max
                  from ${APP}.dwd_sdk_du
                  where dt = '$do_date'
                    and over_num = success_num
                    and cast(conn_type as bigint) = 1) host
                  inner join (
             select case
                        when network like '%移动' then '移动'
                        when network like '%联通' then '联通'
                        when network like '%电信' then '电信'
                        else '其他' end as             network,
                    version                                version,
                    conn_id,
                    cast(first_frame_time as bigint) first_frame_time,
                    cast(dec_stuck_time as bigint)   dec_stuck_time,
                    cast(conn_time as bigint)        conn_time
             from ${APP}.dwd_sdk_su
             where dt = '$do_date'
               and over_num = success_num
               and cast(first_frame_time as bigint) > 0
               and cast(conn_time as bigint) > 0
               and cast(conn_type as bigint) = 1
         ) client on host.conn_id = client.conn_id
     ) tt
group by client_network, host_network,host_version,client_version
order by client_network desc, host_network desc,host_version desc,client_version desc;"

ads_licode_connection_action_4500="
insert overwrite table ${APP}.ads_licode_connection_action_4500
select
    '$do_date',labtype,
    count(1),
    sum(if(array_contains(a1,'1'),1,0)),
    sum(if(array_contains(a2,'1'),1,0)),
    sum(if(array_contains(a3,'1'),1,0)),
    sum(if(array_contains(a4,'1')  or array_contains(a4,'5') or  array_contains(a4,'4') ,1,0)),
    sum(if(array_contains(a4,'1') or array_contains(a4,'5'),1,0))
from
(
        select
            conn_id,
            collect_list(cast(forward_handshake_success as STRING)) a1,
            collect_list(forward_authentication_success) a2,
            collect_list(ec_success) a3,
            collect_list(ea_success) a4
        from ${APP}.dws_backend_licode_day_total_4500
        where dt='$do_date'
	group by conn_id
)t1
inner join
(
    select
        connid,labtype
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    group by connid,labtype
)t3
on t1.conn_id=t3.connid
group by labtype;"

ads_rtc_switch_total="
insert overwrite table ${APP}.ads_rtc_switch_total
select
    '$do_date',
    version,
       cast(count(if((ConnTime2 > 0 and ConnTime3 > 0 and DecStuckTime2 / ConnTime2 - DecStuckTime3 / ConnTime3 >= 0.05) or
                (DecFrMean3 - DecFrMean2 >= 3), DecStuckTime3 / ConnTime3, null)) * 100 /count(1) as DECIMAL(16,2))  as switch_to_better_ratio,

       cast(count(if((ConnTime2 > 0 and ConnTime3 > 0 and DecStuckTime2 / ConnTime2 - DecStuckTime3 / ConnTime3 >= 0.05) and
                (DecFrMean3 - DecFrMean2 >= 3), DecStuckTime3 / ConnTime3, null)) * 100 /count(1) as DECIMAL(16,2))  as switch_to_better_ratio2,

       cast(count(if(ConnTime2 > 0 and ConnTime3 > 0 and DecStuckTime3 / ConnTime3 - DecStuckTime2 / ConnTime2 > 0.05 and
                DecFrMean2 - DecFrMean3 > 3, DecStuckTime3 / ConnTime3, null)) * 100 /count(1) as DECIMAL(16,2))  as switch_to_bad_ratio,
       count(1)
from
(
    select
        conn_id,
        cast(get_json_object(turndata[last_report_index-2], '$.DecFrMean') as DECIMAL(16,2))    as DecFrMean2,
        cast(get_json_object(turndata[last_report_index-1], '$.DecFrMean') as DECIMAL(16,2))       as DecFrMean3,
        cast(get_json_object(turndata[last_report_index-2], '$.RendFrMean') as DECIMAL(16,2))   as RendFrMean2,
        cast(get_json_object(turndata[last_report_index-1], '$.RendFrMean') as DECIMAL(16,2))      as RendFrMean3,
        cast(get_json_object(turndata[last_report_index-2], '$.DecStuckCnt') as bigint)   as DecStuckCnt2,
        cast(get_json_object(turndata[last_report_index-1], '$.DecStuckCnt') as bigint)      as DecStuckCnt3,
        cast(get_json_object(turndata[last_report_index-2], '$.DecStuckTime') as DECIMAL(16,2)) as DecStuckTime2,
        cast(get_json_object(turndata[last_report_index-1], '$.DecStuckTime') as DECIMAL(16,2))    as DecStuckTime3,
        cast(get_json_object(turndata[last_report_index-2], '$.ConnTime') as bigint)      as ConnTime2,
        cast(get_json_object(turndata[last_report_index-1], '$.ConnTime') as bigint)         as ConnTime3
    from
    (
        select
               conn_id,
               split(regexp_replace(
                                  regexp_replace(turndata, '\\\[|\\\]', ''),
                                  '\\\}\\\,\\\{', '\\\}\\\;\\\{'), '\\\;') turndata,
               size(split(regexp_replace(
                                  regexp_replace(turndata, '\\[|\\]', ''),
                                  '\\\}\\\,\\\{', '\\\}\\\;\\\{'), '\\\;')) last_report_index
        from ${APP}.dwd_link_info
        where dt = '$do_date'
        and clientype='client'
          and turndata like '[%'
    )t1
    where last_report_index > 2
    and get_json_object(turndata[last_report_index-2], '$.ConnType') is not null
    and cast(get_json_object(turndata[last_report_index-2], '$.ConnType') as bigint) = 1
    and get_json_object(turndata[last_report_index-1], '$.ConnType') is not null
    and cast(get_json_object(turndata[last_report_index-1], '$.ConnType') as bigint) = 0
    and get_json_object(turndata[last_report_index-2], '$.ConnTime') is not null
    and cast(get_json_object(turndata[last_report_index-2], '$.ConnTime') as bigint) > 0
    and get_json_object(turndata[last_report_index-1], '$.ConnTime') is not null
    and cast(get_json_object(turndata[last_report_index-1], '$.ConnTime') as bigint) >= 60
)t2
inner join
(
    select
        conn_id,version
    from
    (
        select
            conn_id,
            version,
            split(regexp_replace(
                                      regexp_replace(turndata, '\\\[|\\\]', ''),
                                      '\\\}\\\,\\\{', '\\\}\\\;\\\{'), '\\\;') turndata,
            size(split(regexp_replace(
                                      regexp_replace(turndata, '\\\[|\\\]', ''),
                                      '\\\}\\\,\\\{', '\\\}\\\;\\\{'), '\\\;')) last_report_index
        from ${APP}.dwd_link_info
        where dt = '$do_date'
        and clientype='host'
        and turndata like '[%'
        and version >= '4.3.2.1'
    )t1
    where last_report_index > 2
    and get_json_object(turndata[last_report_index-2],'$.UnpubReason') is not null
    and cast(get_json_object(turndata[last_report_index-2], '$.NetReason') as bigint) > 0
    and get_json_object(turndata[last_report_index-2], '$.NetReason') is not null
    and get_json_object(turndata[last_report_index-2], '$.CapFrMean') is not null
    and cast(get_json_object(turndata[last_report_index-2], '$.CapFrMean') as DECIMAL) > 25
    and get_json_object(turndata[last_report_index-2], '$.RttMean') is not null
    and cast(get_json_object(turndata[last_report_index-2], '$.RttMean') as DECIMAL) > 0
    group by conn_id,version
)t3
on t2.conn_id=t3.conn_id
group by version;"

ads_serv_diff_total="
insert overwrite table ${APP}.ads_serv_diff_total
select
    '$do_date',
    t1.serv,rate1,rate2,
    rate2-rate1
from
(
    select
        serv,cast(sum(handshake_success_count)/sum(new_connect_count) *100 as DECIMAL(16,2)) rate1,sum(new_connect_count) new
    from ${APP}.dws_serv_total
    where dt='$do_date'
    group by serv
)t1
left join
(
    select
        serv,cast(sum(handshake_success_count)/sum(new_connect_count) *100 as DECIMAL(16,2)) rate2
    from ${APP}.dws_serv_total
    where dt=date_add('$do_date',-7)
    group by serv
)t2
on t1.serv=t2.serv
where rate2-rate1>=3 and new > 1000;"

ads_bad_serv_top="
insert overwrite table ${APP}.ads_bad_serv_top
select
    *
from(
select
    '$do_date',
    serv,
    sum(handshake_success_count),
    sum(authentication_success_count),
    sum(new_connect_count),
    cast(sum(handshake_success_count)/sum(new_connect_count) *100 as DECIMAL(16,2)) rate1,
    cast(sum(authentication_success_count)/sum(new_connect_count) *100 as DECIMAL(16,2)) rate2
from ${APP}.dws_serv_total
where dt='$do_date'
and serv != 'BGP_sh_2-BGP_sh_2'
group by serv
)t1
where rate1<=90
order by rate1;"

ads_client_newuser_newsu="
insert overwrite table ${APP}.ads_client_newuser_newsu
select
    '$do_date',
    client_type,
    sum(if(first_su_date='$do_date',1,0)),
    sum(if(last_su_date='$do_date',1,0))
from ${APP}.dwt_su_new_total_topic
where dt = '$do_date'
and client_type is not null
group by client_type;"

ads_client_newuser_suretention="
insert overwrite table ${APP}.ads_client_newuser_suretention
select * from ${APP}.ads_client_newuser_suretention
union
select *
from
(   
    select
        '$do_date',
        first_su_date d1,
        client_type,
        datediff('$do_date',first_su_date) c1,
        sum(if(last_su_date='$do_date',1,0)),
        count(*) 
    from ${APP}.dwt_su_new_total_topic
    where dt='$do_date'
    and first_su_date>=date_add('$do_date',-30)
    and first_su_date<'$do_date'
    group by first_su_date,client_type
)t1
where t1.c1 in(1,2,3,4,5,6,7,14,30);"

ads_client_newuser_newdu="
insert overwrite table ${APP}.ads_client_newuser_newdu
select
    '$do_date',
    client_type,
    sum(if(first_du_date='$do_date',1,0)),
    sum(if(last_du_date='$do_date',1,0))
from ${APP}.dwt_du_new_total_topic
where dt = '$do_date'
and client_type is not null
group by client_type;"

ads_client_newuser_duretention="
insert overwrite table ${APP}.ads_client_newuser_duretention
select * from ${APP}.ads_client_newuser_duretention
union
select *
from
(
    select
        '$do_date',
        first_du_date d1,
        client_type,
        datediff('$do_date',first_du_date) c1,
        sum(if(last_du_date='$do_date',1,0)),
        count(*)
    from ${APP}.dwt_du_new_total_topic
    where dt='$do_date'
    and first_du_date>=date_add('$do_date',-30)
    and first_du_date<'$do_date'
    group by first_du_date,client_type
)t1
where t1.c1 in(1,2,3,4,5,6,7,14,30);"

ads_bigdata_datas_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_bigdata_datas_total
select
    '$do_date','ods_tuevent_log',count(1)
from ${APP}.ods_tuevent_log
where dt='$do_date'
union all
select
    '$do_date','ods_click_log',count(1)
from ${APP}.ods_click_log
where dt='$do_date'
union all
select
    '$do_date','ods_client_log',count(1)
from ${APP}.ods_client_log
where dt='$do_date'
union all
select
    '$do_date','ods_tevent_log',count(1)
from ${APP}.ods_tevent_log
where dt='$do_date'
union all
select
    '$do_date','ods_event_log',count(1)
from ${APP}.ods_event_log
where dt='$do_date'
union all
select
    '$do_date','ods_now_log',count(1)
from ${APP}.ods_now_log
where dt='$do_date'
union all
select
    '$do_date','ods_other_log',count(1)
from ${APP}.ods_other_log
where dt='$do_date';"

ads_rtc_rpru_total="
insert overwrite table ${APP}.ads_rtc_rpru_total
select
    '$do_date',
    *,
    cast(rp2ru_count / rp_count *100 as DECIMAL(16,2))
from
(
    select
        version,
        count(1) all_count,
        sum(if(last_report_index>0
            and get_json_object(turndata[last_report_index-1],'$.ConnType')='0'
            and cast(get_json_object(turndata[last_report_index-1],'$.ConnTime')as bigint  )> 0,1,0)) ru_count,
        sum(if(last_report_index>1
            and get_json_object(turndata[1],'$.ConnType')='1'
            and get_json_object(turndata[0],'$.ConnType')='0'
            and cast(get_json_object(turndata[1],'$.ConnTime')as bigint  )> 0,1,0)) rp_count,
        sum(if(last_report_index>2
            and cast(get_json_object(turndata[1],'$.NetReason')as bigint  )> 0
            and get_json_object(turndata[last_report_index-1],'$.ConnType')='0'
            and cast(get_json_object(turndata[last_report_index-1],'$.ConnTime')as bigint  )> 0,1,0)) rp2ru_count
    from
    (
        select
            conn_id,
            version,
	    split(regexp_replace(
                                  regexp_replace(turndata, '\\\[|\\\]', ''),
                                  '\\\}\\\,\\\{', '\\\}\\\;\\\{'), '\\\;') turndata,
               size(split(regexp_replace(
                                  regexp_replace(turndata, '\\[|\\]', ''),
                                  '\\\}\\\,\\\{', '\\\}\\\;\\\{'), '\\\;')) last_report_index
        from ${APP}.dwd_link_info
        where dt = '$do_date'
        and clientype='host'
        and turndata like '[%'
        and version >= '4.3.2.1'
    )t1
    group by version
)t2;"

ads_pc_new_version_total="
insert overwrite table ${APP}.ads_pc_new_version_total 
select
    '$do_date',version,count(1)
from
(
    select
        id
    from ${APP}.dwt_mac_topic
    where dt='$do_date'
    and first_login_date='$do_date'
    group by id
)t1
left join
(
    select
        mac_id,
        version
    from ${APP}.dwd_client_event
    where dt = '$do_date'
    and mac_id is not null
    and mac_id <> ''
    and action in('start_front','start_process')
    group by mac_id,version
)t2
on t1.id=t2.mac_id
group by t2.version;"

ads_backend_ec_error_reason_all_4500="
insert overwrite table ${APP}.ads_backend_ec_error_reason_all_4500
select
    t1.dt,reason,s1,cast (s1/authentication_success_count *100 as decimal(16,2))
from
(
    select
        '$do_date' dt,reason,sum(c1) s1
    from
    (
        
        select
            '1',split(failed_reason,'_')[1] reason,c1
        from
        (
            select
                failed_reason,count(1) c1
            from
            (
                select
                    conn_id,failed_reason
                from ${APP}.dws_backend_ec_error_total_4500
                where dt = '$do_date'
                group by conn_id, failed_reason
            )t1
            group by failed_reason
        )t3
        where split(failed_reason,'_')[0] in ('presenter','viewer') and failed_reason not like '%-%'
        union all
        
        select
            '1',failed_reason,c1
        from
        (
            select
                failed_reason,count(1) c1
            from
            (
                select
                    conn_id,failed_reason
                from ${APP}.dws_backend_ec_error_total_4500
                where dt = '$do_date'
                group by conn_id, failed_reason
            )t1
            group by failed_reason
        )t3
        where failed_reason in ('other','loss data')
        union all
        
        select
            type,'both',sum(c1)
        from
         (
            select
                '2' type,failed_reason,if(split(reason,'-')[1]='',concat_ws('',reason,'other'),reason),c1
            from
            (
                select
                    failed_reason,split(failed_reason,'-')[0],if(split(failed_reason,'-')[0] ='',concat_ws('','other',failed_reason),failed_reason) reason,c1
                from
                (
                    select
                        failed_reason,count(1) c1
                    from
                    (
                        select
                            conn_id,failed_reason
                        from ${APP}.dws_backend_ec_error_total_4500
                        where dt = '$do_date'
                        group by conn_id, failed_reason
                    )t1
                    group by failed_reason
                )t3
                where failed_reason like '%-%'
            )t4
         )t5
         group by type
    )tt
    group by reason
)t
left join
(
    select
        dt,authentication_success_count
    from ${APP}.ads_licode_connection_action_4500
)t1
on t1.dt=t.dt;"

ads_backend_ec_error_reason_both_4500="
insert overwrite table ${APP}.ads_backend_ec_error_reason_both_4500
select
    '$do_date',re,s1,cast(s1/sum(s1) over() *100 as DECIMAL(16,2))
from
(
    select
        re,sum(c1) s1
    from
    (
        select
            reason,if(split(reason,'-')[0] rlike '.*_.*',split(split(reason,'-')[0],'_')[1],split(reason,'-')[0]) re,c1
        from
        (
            select
                failed_reason,if(split(reason,'-')[1]='',concat_ws('',reason,'other'),reason) reason,c1
            from
            (
                select
                    failed_reason,split(failed_reason,'-')[0],if(split(failed_reason,'-')[0] ='',concat_ws('','other',failed_reason),failed_reason) reason,c1
                from
                (
                    select
                        failed_reason,count(1) c1
                    from
                    (
                        select
                            conn_id,failed_reason
                        from ${APP}.dws_backend_ec_error_total_4500
                        where dt = '$do_date'
                        group by conn_id, failed_reason
                    )t1
                    group by failed_reason
                )t3
                where failed_reason like '%-%'
            )t4
        )t5
        union all
        select
            reason,if(split(reason,'-')[1] rlike '.*_.*',split(split(reason,'-')[1],'_')[1],split(reason,'-')[1]),c1
        from
        (
            select
                failed_reason,if(split(reason,'-')[1]='',concat_ws('',reason,'other'),reason) reason,c1
            from
            (
                select
                    failed_reason,split(failed_reason,'-')[0],if(split(failed_reason,'-')[0] ='',concat_ws('','other',failed_reason),failed_reason) reason,c1
                from
                (
                    select
                        failed_reason,count(1) c1
                    from
                    (
                        select
                            conn_id,failed_reason
                        from ${APP}.dws_backend_ec_error_total_4500
                        where dt = '$do_date'
                        group by conn_id, failed_reason
                    )t1
                    group by failed_reason
                )t3
                where failed_reason like '%-%'
            )t4
        )t5
    )tt
    group by re
)t;"

ads_backend_ea_error_reason_all_4500="
insert overwrite table ${APP}.ads_backend_ea_error_reason_all_4500
select
    t3.dt,failed_reason,c1,cast(c1/ec_success_count *100 as decimal(16,2))
from
 (
    select
        '$do_date' dt,failed_reason,count(1) c1
    from
    (
        select
            stream_id,failed_reason
        from ${APP}.dws_backend_ea_failed_total_4500
        where dt='$do_date'
        and failed_reason not like '%-%'
        group by stream_id,failed_reason
    )t1
    group by failed_reason
    union all
    select
        '$do_date','both',sum(c1)
    from
    (
        select
            failed_reason,count(1) c1
        from
        (
            select
                stream_id,failed_reason
            from ${APP}.dws_backend_ea_failed_total_4500
            where dt='$do_date'
            and failed_reason  like '%-%'
            group by stream_id,failed_reason
        )t1
        group by failed_reason
    )tt
 )t3
 left join
 (
     select
        dt,ec_success_count
    from ${APP}.ads_licode_connection_stream_4500
 )t4
 on t3.dt=t4.dt;"

ads_backend_ea_error_reason_both_4500="
insert overwrite table ${APP}.ads_backend_ea_error_reason_both_4500
select
    '$do_date',
    failed_reason,
    c1,
    cast(c1 / sum(c1) over() *100 as DECIMAL(16,2))
from
(
    select
        failed_reason,
        sum(c1) c1
    from
    (
        select
            if(split(failed_reason,'-')[0] rlike '.*_.*',split(split(failed_reason,'-')[0],'_')[1],split(failed_reason,'-')[0]) failed_reason,count(1) c1
        from
        (
            select
                stream_id,failed_reason
            from ${APP}.dws_backend_ea_failed_total_4500
            where dt='$do_date'
            and failed_reason like '%-%'
            group by stream_id,failed_reason
        )t1
        group by failed_reason
        union all
        select
            if(split(failed_reason,'-')[1] rlike '.*_.*',split(split(failed_reason,'-')[1],'_')[1],split(failed_reason,'-')[1]) failed_reason,count(1) c1
        from
        (
            select
                stream_id,failed_reason
            from ${APP}.dws_backend_ea_failed_total_4500
            where dt='$do_date'
            and failed_reason like '%-%'
	    and split(failed_reason,'-')[0] <> 'viewer_transport close'
            group by stream_id,failed_reason
        )t1
        group by failed_reason
    )t3
    group by failed_reason
)t4;"

ads_licode_connection_stream_4500="
insert overwrite table ${APP}.ads_licode_connection_stream_4500
select
    '$do_date',sum(if(ea_success ='1',1,0)),count(1)
from
(
    select
        conn_id,stream_id,ea_success
    from ${APP}.dws_backend_licode_ea_total_4500
    where dt='$do_date'
    group by conn_id,stream_id,ea_success
)t1;"

ads_serv_recover_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_serv_recover_total
select
    '$do_date',
    t1.serv,rate1,rate2,
    rate2-rate1,rate3,rate3-rate1
from
(
    select
        t1.serv,rate1
    from
    (
        select
            serv,cast(sum(handshake_success_count)/sum(new_connect_count) *100 as DECIMAL(16,2)) rate1,sum(new_connect_count) new
        from ${APP}.dws_serv_total
        where dt=date_add('$do_date',-1)
        group by serv
    )t1
    left join
    (
        select
            serv,cast(sum(handshake_success_count)/sum(new_connect_count) *100 as DECIMAL(16,2)) rate2
        from ${APP}.dws_serv_total
        where dt=date_add('$do_date',-8)
        group by serv
    )t2
    on t1.serv=t2.serv
    where rate2-rate1>=3 and new > 1000
)t1
left join
(
    select
        serv,cast(sum(handshake_success_count)/sum(new_connect_count) *100 as DECIMAL(16,2)) rate2
    from ${APP}.dws_serv_total
    where dt='$do_date'
    group by serv
)t2
on t1.serv=t2.serv
left join
(
    select
        serv,cast(sum(handshake_success_count)/sum(new_connect_count) *100 as DECIMAL(16,2)) rate3
    from ${APP}.dws_serv_total
    where dt=date_add('$do_date',-7)
    group by serv
)t3
on t1.serv=t3.serv;"

ads_product_banner_total="
insert overwrite table ${APP}.ads_product_banner_total
select
    '$do_date',tt.platform,tt.showbanner,tt.id,if(c2 is not null,c2,0),c1,cast(if(c2 is not null,c2,0)/c1 *100 as DECIMAL(16,4))
from
(
    select
        platform,showbanner, id, count(1) c1
    from
    (
        select
            platform,get_json_object(ck_url,'$.showbanner') showbanner,get_json_object(ck_url,'$.id') id,account_id,count(1)
        from ${APP}.dwd_client_pageview
        where dt='$do_date'
        and module='banner'
        and platform='windows'
        and get_json_object(ck_url,'$.showbanner')  is not null
        and get_json_object(ck_url,'$.id') is not null
        group by platform,get_json_object(ck_url,'$.showbanner'),get_json_object(ck_url,'$.id'),account_id
    )t1
    group by platform,showbanner,id
)tt
left join
(
    select
        clickbanner,id,count(1) c2
    from
    (
        select
            get_json_object(ck_url,'$.clickbanner') clickbanner,get_json_object(ck_url,'$.id') id,account_id
        from ${APP}.dwd_client_click
        where dt='$do_date'
        and module='banner'
        and platform='windows'
        and get_json_object(ck_url,'$.clickbanner') is not null
        and get_json_object(ck_url,'$.id') is not null
        group by get_json_object(ck_url,'$.clickbanner'),get_json_object(ck_url,'$.id'),account_id
    )t1
    group by clickbanner,id
)tt1
on tt.id=tt1.id
union all
select
    '$do_date',tt.platform,tt.showbanner,tt.id,if(c2 is not null,c2,0),c1,cast(if(c2 is not null,c2,0)/c1 *100 as DECIMAL(16,4))
from
(
    select
        platform,showbanner, id, count(1) c1
    from
    (
        select
            platform,get_json_object(ck_url,'$.showbanner') showbanner,get_json_object(ck_url,'$.imageId') id,account_id,count(1)
        from ${APP}.dwd_client_pageview
        where dt='$do_date'
        and module in ('my','home')
        and action in ('banner','window')
        and ((platform='android' and version<'4.7.0.1') or (platform='iOS' and version<'4.7.08'))
        and get_json_object(ck_url,'$.showbanner')  is not null
        and get_json_object(ck_url,'$.imageId') is not null
        group by platform,get_json_object(ck_url,'$.showbanner'),get_json_object(ck_url,'$.imageId'),account_id
    )t1
    group by platform,showbanner,id
)tt
left join
(
    select
        platform,clickbanner,id,count(1) c2
    from
    (
        select
            platform,if(get_json_object(ck_url,'$.clickbanner') is not null,get_json_object(ck_url,'$.clickbanner'),get_json_object(ck_url,'$.clickwindow'))  clickbanner,get_json_object(ck_url,'$.imageId') id,account_id
        from ${APP}.dwd_client_click
        where dt='$do_date'
        and module in ('my','home')
        and action in ('banner','window')
	and ((platform='android' and version<'4.7.0.1') or (platform='iOS' and version<'4.7.08'))
	and (get_json_object(ck_url,'$.clickbanner') is not null or get_json_object(ck_url,'$.clickwindow')='floatwindow')
        and get_json_object(ck_url,'$.imageId') is not null
        group by platform,if(get_json_object(ck_url,'$.clickbanner') is not null,get_json_object(ck_url,'$.clickbanner'),get_json_object(ck_url,'$.clickwindow')),get_json_object(ck_url,'$.imageId'),account_id
    )t1
    group by platform,clickbanner,id
)tt1
on tt.id=tt1.id and tt.platform=tt1.platform and tt.showbanner=tt1.clickbanner
union all
select
    '$do_date',t1.platform,t1.a,t1.b,if(c2 is not null,c2,0),c1,cast(if(c2 is not null,c2,0)/c1 *100 as DECIMAL(16,4))
from
(
    select
        platform,a,b,count(1) c1
    from
    (
        select platform,get_json_object(get_json_object(ck_url,'$.banner_view_from'),'$.a') a,get_json_object(get_json_object(ck_url,'$.banner_view_from'),'$.b') b
        from ${APP}.dwd_client_pageview
        where dt='$do_date'
        and action='from'
        and ((platform='android' and version>='4.7.0.1') or (platform='iOS' and version>='4.7.08'))
        and get_json_object(get_json_object(ck_url,'$.banner_view_from'),'$.a') is not null
        and get_json_object(get_json_object(ck_url,'$.banner_view_from'),'$.b') is not null
    )t11
    group by platform,a,b
)t1
left join
(
    select
        platform,a,b,count(1) c2
    from
    (
        select
            platform,get_json_object(get_json_object(ck_url,'$.banner_click_from'),'$.a') a,get_json_object(get_json_object(ck_url,'$.banner_click_from'),'$.b') b
        from ${APP}.dwd_client_click
        where dt='$do_date'
        and action='from'
        and ((platform='android' and version>='4.7.0.1') or (platform='iOS' and version>='4.7.08'))
        and get_json_object(get_json_object(ck_url,'$.banner_click_from'),'$.a') is not null
        and get_json_object(get_json_object(ck_url,'$.banner_click_from'),'$.b') is not null
        and get_json_object(get_json_object(ck_url,'$.banner_click_from'),'$.c') ='open'
    )t21
    group by platform,a,b
)t2
on t1.platform=t2.platform and t1.a=t2.a and t1.b=t2.b;"

ads_mac_avg_total="
insert overwrite table ${APP}.ads_mac_avg_total
select
    '$do_date',cast(avg(success_cnt) as DECIMAL(16,2)),cast(avg(connect_cnt) as DECIMAL(16,2)),cast(avg(success_rate) as DECIMAL(16,2))
from
(
    select
        suid,sum(if(t2.connid is not null,1,0)) success_cnt,count(1) connect_cnt,cast(sum(if(t2.connid is not null,1,0))/count(1) *100 as DECIMAL(16,4)) success_rate
    from
    (
        select
            suid,connid
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        and enterprise='0'
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
    group by suid
)t3;"

ads_serv_all_not_present_total="
with tmp as
(
    select
        connid,relayservername, ts, after_ts,if(after_ts-ts<=10*1000,'1','0') c1,date_format(from_utc_timestamp(ts,'Asia/Shanghai'),'yyyy-MM-dd HH:mm') ts1
    from
    (
        select 
            connid,relayservername,ts,lead(ts,9) over(partition by relayservername order by ts) after_ts
        from  ${APP}.dwd_connection_failure
        where dt='$do_date'
        and failure_type ='all_not_present'
    )t1
)
insert overwrite table ${APP}.ads_serv_all_not_present_total
select
    '$do_date',relayservername, time_flag,count(1)
from
(
    select
        relayservername,concat(min,'--',from_unixtime(unix_timestamp(concat(day,' ',concat(min,':00')),'yyyy-MM-dd HH:mm:ss') + 5*60,'HH:mm')) time_flag
    from
    (
        select
            connid,relayservername,day,
            case when min='' then min_test
                 else min end min
        from
         (
            select
                connid,relayservername,day,
                case when min =0 then concat(hour,':','00' )
                     when min =1 then concat(hour,':','05' )
                     else  '' end min,
                concat(hour,':',min*5 ) min_test
            from
            (
                select
                    connid,relayservername,date_format(from_utc_timestamp(ts,'Asia/Shanghai'),'yyyy-MM-dd') day,date_format(from_utc_timestamp(ts,'Asia/Shanghai'),'HH') hour,
                    \`floor\`(cast(date_format(from_utc_timestamp(ts,'Asia/Shanghai'),'mm') as int) / 5) min
                from
                (
                    select  
                        t1.connid,t1.relayservername,t1.ts
                    from
                    (
                        select
                            connid,relayservername,ts
                        from tmp
                        where ts
                    )t1
                    left join
                    (
                        select 
                            relayservername,ts,after_ts
                        from tmp
                        where c1='1'
                    )t2
                    on t1.relayservername=t2.relayservername
                    where t1.ts>=t2.ts and t1.ts<=t2.after_ts
                    group by t1.connid,t1.relayservername,t1.ts
                )t3
            )t4
         )tt
    )t5
)t6
group by relayservername,time_flag
order by relayservername,time_flag;"

dwd_anti_fraud_data="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_anti_fraud_data
select
    suid,sclient,sip,sport,scountry,sprovince,scity,t5.phone_num,t5.email,duid,dip,dport,dprovince,dcity,t6.phone_num,t6.email,ts,\`if\`(link_time is not null,link_time,''),black_110
from
(
    select
        suid,duid,sclient,sip,sport,scountry,sprovince,scity,dip,dport,dprovince,dcity,phone_num,email,\`duser\`,ts,link_time,black_110
    from
    (
        select
            suid,duid,sclient,sip,sport,scountry,sprovince,scity,dip,dport,dprovince,dcity,\`user\`,\`duser\`,ts,link_time,black_110
        from
        (
            select
                connid,sclient,suid,duid,sip,sport,dip,dport,scountry,sprovince,scity,dprovince,dcity,\`user\`,duser,date_format(from_utc_timestamp(ts,'Asia/Shanghai'),'yyyy-MM-dd HH:mm:ss') ts ,black_110
            from ${APP}.dwd_new_connect
            where dt ='$do_date'
            and (scountry in ('越南','老挝','柬埔寨','泰国','缅甸','马来西亚','新加坡','印度尼西亚') or sprovince in ('中国台湾','云南省'))
            and dcountry='中国'
            and dclient='ANDROID'
	    and black_110 is not null
        )t1
        left join
        (
            select
                connid,link_time
            from ${APP}.dwd_session_success
            where dt = '$do_date'
        )t2
        on t1.connid=t2.connid
    )t3
    left join
    (
        select
            id,phone_num,email
        from ${APP}.dim_user_info
        where dt='$do_date'
        and (id is not null or id <> '' or phone_num is not null )
    )t4
    on t3.\`user\`=t4.id
)t5
left join
(
    select
        id,phone_num,email
    from ${APP}.dim_user_info
    where dt='$do_date'
    and (id is not null or id <> '' or phone_num is not null )
)t6
on t5.duser=t6.id;"

dwd_anti_fraud_order="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dwd_anti_fraud_order
select
    t1.*,if(t2.phone is null,'',t2.phone)
from
(
    select
        user_id,trade_id,total_fee/100,
        case
            when channel=1 then '支付宝'
            when channel=2 then '微信'
            when channel=3 then '苹果' else 'other' end channel,
        case
            when order_type=0 then '购买'
            when order_type=1 then '续费'
            when order_type=2 then '升级'
            when order_type=3 then '试用'
            when order_type=4 then '增购' else 'other' end order_type,
        date_format(from_utc_timestamp(pay_time *1000,'Asia/Shanghai'),'yyyy-MM-dd HH:mm:ss') pay_time
    from ${APP}.ods_tv_bill_order
    where dt='$do_date'
    and status=2
    and pay_status=1
    and order_type !=3
)t1
left join
(
    select
        id,
        if(phone_num rlike '^\\\\d+$' and length(phone_num)=11,phone_num,email) phone
    from ${APP}.ods_user_info
    where dt='$do_date'
)t2
on t1.user_id=t2.id;"

ads_licode_sfu_total="
with tmp as
(
    select
        t5.*,t6.enterprise
    from
     (
        select
            *,nvl(sub_conn,pub_conn) conn
        from
        (
            select
                t1.connid pub_conn,t1.streamid pub_str
            from
            (
                select
                    split(connid,'_')[2] connid, streamid
                from ${APP}.dwd_backend_licode_publish_ready
                where dt='$do_date'
                and is_sfu='true'
                and size(split(connid,'_'))=3
                group by split(connid,'_')[2],streamid
            )t1
            inner join
            (
                select
                    split(connid,'_')[2] connid
                from ${APP}.dwd_backend_licode_publish_start
                where dt='$do_date'
                and is_sfu='true'
                and size(split(connid,'_'))=3
                group by split(connid,'_')[2]
            )t2
            on t1.connid=t2.connid
        )t3
        full outer join
        (
            select
                t1.connid sub_conn,t1.streamid sub_str
            from
            (
                select
                    split(connid,'_')[2] connid,streamid
                from ${APP}.dwd_backend_licode_subscribe_sfu
                where dt='$do_date'
                and is_sfu='true'
                and size(split(connid,'_'))=3
                group by split(connid,'_')[2],streamid
            )t1
            inner join
            (
                select
                    split(connid,'_')[2] connid
                from ${APP}.dwd_backend_licode_subscribe_ready
                where dt='$do_date'
                and is_sfu='true'
                and size(split(connid,'_'))=3
                group by split(connid,'_')[2]
            )t2
            on t1.connid=t2.connid
        )t4
        on t3.pub_str=t4.sub_str
     )t5
    left join
     (
        select
            connid,enterprise
         from ${APP}.dwd_new_connect
         where dt='$do_date'
     )t6
    on  t5.conn=t6.connid
)
insert overwrite  table ${APP}.ads_licode_sfu_total
select 
    '$do_date',*
from
(
    select
        case when enterprise ='1' then 'b_pub'
            when enterprise ='0' then 'c_pub'
        else 'other_pub' end ,count(distinct pub_conn)
    from tmp
    where tmp.pub_conn is not null
    and tmp.sub_conn is not null
    group by enterprise
    union all
    select
        case when enterprise ='1' then 'b_sub'
            when enterprise ='0' then 'c_sub'
        else 'other_sub' end ,count(distinct sub_conn)
    from tmp
    where tmp.pub_conn is not null
    and tmp.sub_conn is not null
    group by enterprise
    union all
    select
        'all',count(distinct sub_str)
    from tmp
    where tmp.pub_conn is not null
    and tmp.sub_conn is not null
    union all
    select
        cast(c1 as STRING),count(1)
    from
    (
        select
            sub_str,count(1) c1
        from tmp
        where tmp.pub_conn is not null
        and tmp.sub_conn is not null
        group by sub_str
    )t1
    group by cast(c1 as STRING)
)t11;"

ads_mobile_vedio_total="
insert overwrite table ${APP}.ads_mobile_vedio_total
select
    '$do_date',
    platform,
    if(version is null ,'all',version),
    if(type is null ,'all',type),s1,c1,r1
from
(
    select
        platform,
        version,
        type,
        sum(if(fst > 0,1,0 )) s1,
        count(1) c1,
        cast(sum(if(fst > 0,1,0 ))/count(1) *100 as decimal(16,2)) r1
    from
    (
        select
            platform,
            version,
            case
             when is_rtc = '1' and conn_type = '0' then 'erizo'
             when is_rtc = '1' and conn_type = '1' then 'p2p'
             when is_rtc = '0' then 'fallback'
            else 'other' end as type,
            cast(first_frame_time as bigint) fst
        from ${APP}.dwd_rtc_android_su
        where dt='$do_date'
        and  success_num=over_num
        and platform is not null
        and version is not null
    )t1
    group by platform,version,type grouping sets ((platform,version,type),(platform,type),platform)
)t2;"

ads_mobile_conn_fst_time_total="
insert overwrite table ${APP}.ads_mobile_conn_fst_time_total
select
     '$do_date',
    platform,
    if(version is null ,'all',version),
    if(type is null ,'all',type),fst_mean,fst1_mean,fst2_mean,fst3_mean,fst4_mean,
    concat_ws(',', cast(cast(fstTimePercentile[0] as BIGINT) AS String),
                 cast(cast(fstTimePercentile[1] as BIGINT) AS String),
                 cast(cast(fstTimePercentile[2] as BIGINT) AS String),
                 cast(cast(fstTimePercentile[3] as BIGINT) AS String),
                 cast(cast(fstTimePercentile[4] as BIGINT) AS String)),
    avgStuckRatio,
    rcvdBps,
    headBps
from (
     select
            platform,
            version,
            type,
            cast(avg(fst) as decimal(16,2)) fst_mean,
            cast(avg(fst1) as decimal(16,2)) fst1_mean,
            cast(avg(fst2) as decimal(16,2)) fst2_mean,
            cast(avg(fst3) as decimal(16,2)) fst3_mean,
            cast(avg(fst4) as decimal(16,2)) fst4_mean,
            percentile_approx(cast(fst as BIGINT),
                              array(0.1, 0.3, 0.5, 0.7, 0.9)) as                   fstTimePercentile,
            cast(avg(dec_stuck_time /  conn_time * 100) as DECIMAL(16, 2)) avgStuckRatio,
            cast(avg( (pbytes+hbytes) *8 / conn_time ) as decimal(16,2)) rcvdBps,
            cast(avg( hbytes * 8 / conn_time ) as decimal(16,2)) headBps
     from (
              select
                     platform,
                     version,
                     case
                         when conn_type = '0' then 'erizo'
                         when conn_type = '1' then 'p2p'
                         else 'other' end as type,
                     cast(first_frame_time as bigint) fst,
                     cast(fst1 as bigint) fst1,
                     cast(fst2 as bigint) fst2,
                     cast(fst3 as bigint) fst3,
                     cast(fst4 as bigint) fst4,
                     cast(cast(dec_stuck_time as bigint) / 1000 as DECIMAL(16,2)) dec_stuck_time,
                     cast(cast(conn_time as bigint) / 1000 as DECIMAL(16,2)) conn_time,
                     cast(pbytes as bigint) pbytes,
                     cast(hbytes as bigint) hbytes
              from ${APP}.dwd_rtc_android_su
              where dt='$do_date'
              and platform is not null
              and version is not null
              and is_rtc='1'
              and cast(first_frame_time as BIGINT) >= -1
          ) t1
     group by platform,version,type grouping sets ((platform,version,type),(platform,type),platform)
 ) t2;"

ads_mobile_dec_total="
 insert overwrite table ${APP}.ads_mobile_dec_total
select
    '$do_date',d,cast(c1/sum(c1) over() *100 as DECIMAL(16,2)),rcvd_fr_mean,dec_fr_mean
from
(
    select
        \`dec\` d,count(1) c1,
        cast(avg(rcvd_fr_mean) as DECIMAL(16,2)) rcvd_fr_mean,
        cast(avg(dec_fr_mean) as DECIMAL(16,2)) dec_fr_mean
    from ${APP}.dwd_rtc_android_su
    where dt='$do_date'
    and \`dec\` is not null 
    and is_rtc='1'
    and \`dec\` <> ''
    group by \`dec\` 
)t2;"

ads_mobile_enc_total="
insert overwrite table ${APP}.ads_mobile_enc_total
select
       '$do_date',enc,codec_name,if(enc_err_code is not null,enc_err_code,'ok'),count(1)
 from ${APP}.dwd_rtc_android_du
 where dt = '$do_date'
 and version is not null
 and over_num = success_num
 and is_rtc = '1'
 and enc is not null
 and codec_name is not null
 group by enc,codec_name,enc_err_code;"

ads_mobile_rtc_cap_rate="
insert overwrite table ${APP}.ads_mobile_rtc_cap_rate
select '$do_date',
       platform,
       if(version is null,'all',version),
       if(type is null,'all',type),
       cap_fr_mean,
       send_fr_mean,
       concat_ws(',', cast(cast(cap_fr_ercentile[0] as BIGINT) AS String),
                 cast(cast(cap_fr_ercentile[1] as BIGINT) AS String),
                 cast(cast(cap_fr_ercentile[2] as BIGINT) AS String),
                 cast(cast(cap_fr_ercentile[3] as BIGINT) AS String),
                 cast(cast(cap_fr_ercentile[4] as BIGINT) AS String)),
       concat_ws(',', cast(cast(send_fr_ercentile[0] as BIGINT) AS String),
                 cast(cast(send_fr_ercentile[1] as BIGINT) AS String),
                 cast(cast(send_fr_ercentile[2] as BIGINT) AS String),
                 cast(cast(send_fr_ercentile[3] as BIGINT) AS String),
                 cast(cast(send_fr_ercentile[4] as BIGINT) AS String))
from
(
    select
        platform,
        version,
        type,
        cast(avg(cap_fr_mean) as DECIMAL(16, 2)) cap_fr_mean,
        cast(avg(send_fr_mean) as DECIMAL(16, 2)) send_fr_mean,
        percentile_approx(cast(cap_fr_mean as BIGINT), array(0.1, 0.3, 0.5, 0.7, 0.9))  as cap_fr_ercentile,
        percentile_approx(cast(send_fr_mean as BIGINT), array(0.1, 0.3, 0.5, 0.7, 0.9))  as send_fr_ercentile
    from
     (
        select
           case
                when conn_type = '0' then 'erizo'
                when conn_type = '1' then 'p2p'
                else 'other' end as              type,
            cast(cap_fr_mean as DECIMAL(16, 2))  cap_fr_mean,
            cast(send_fr_mean as DECIMAL(16, 2)) send_fr_mean,
            version,
            platform
         from ${APP}.dwd_rtc_android_du
         where dt = '$do_date'
           and version is not null
           and over_num = success_num
           and is_rtc = '1'
     )tt
    group by platform,version, type grouping sets (platform,(platform,version),(platform,version,type))
) t1;"

ads_mobile_rtc_tar_rate="
insert overwrite table ${APP}.ads_mobile_rtc_tar_rate
select '$do_date',
        platform,
       if(version is null,'all',version),
       if(type is null,'all',type),
       avgTarRate,
       avgActRate,
       avgSendRate,
       concat_ws(',', cast(cast(tarRatePercentile[0] as BIGINT) AS String),
                 cast(cast(tarRatePercentile[1] as BIGINT) AS String),
                 cast(cast(tarRatePercentile[2] as BIGINT) AS String),
                 cast(cast(tarRatePercentile[3] as BIGINT) AS String),
                 cast(cast(tarRatePercentile[4] as BIGINT) AS String)),
       concat_ws(',', cast(cast(actRatePercentile[0] as BIGINT) AS String),
                 cast(cast(actRatePercentile[1] as BIGINT) AS String),
                 cast(cast(actRatePercentile[2] as BIGINT) AS String),
                 cast(cast(actRatePercentile[3] as BIGINT) AS String),
                 cast(cast(actRatePercentile[4] as BIGINT) AS String)),
       concat_ws(',', cast(cast(sendRatePercentile[0] as BIGINT) AS String),
                 cast(cast(sendRatePercentile[1] as BIGINT) AS String),
                 cast(cast(sendRatePercentile[2] as BIGINT) AS String),
                 cast(cast(sendRatePercentile[3] as BIGINT) AS String),
                 cast(cast(sendRatePercentile[4] as BIGINT) AS String))
from (
         select
                platform,
                version,
                type,
                cast(avg(cast(tar_rate_mean AS DECIMAL(16, 2))) as BIGINT)                        as avgTarRate,
                cast(avg(cast(act_rate_mean AS DECIMAL(16, 2))) as BIGINT)                        as avgActRate,
                cast(avg(cast(send_rate_mean AS DECIMAL(16, 2))) as BIGINT)                       as avgSendRate,
                percentile_approx(cast(tar_rate_mean as BIGINT), array(0.1, 0.3, 0.5, 0.7, 0.9))  as tarRatePercentile,
                percentile_approx(cast(act_rate_mean as BIGINT), array(0.1, 0.3, 0.5, 0.7, 0.9))  as actRatePercentile,
                percentile_approx(cast(send_rate_mean as BIGINT), array(0.1, 0.3, 0.5, 0.7, 0.9)) as sendRatePercentile
         from (
                  select
                         platform,
                         version,
                         case
                             when conn_type = '0' then 'erizo'
                             when conn_type = '1' then 'p2p'
                             else 'other' end as type,
                         tar_rate_mean,
                         act_rate_mean,
                         send_rate_mean
                  from ${APP}.dwd_rtc_android_du
                  where dt = '$do_date'
                    and version is not null
                    and over_num = success_num
                    and is_rtc='1'
              ) t1
         group by platform,version, type grouping sets (platform,(platform,version),(platform,version,type))
     ) t2;"

ads_mobile_rtc_rtt="
insert overwrite table ${APP}.ads_mobile_rtc_rtt
select
       '$do_date',
       platform,
       if(version is null,'all',version),
       if(type is null,'all',type),
       avg_rtt_max,
       avg_rtt_mean,
       avg_rtt_min,
       concat_ws(',', cast(cast(rtt_std_percentile[0] as BIGINT) AS String),
                 cast(cast(rtt_std_percentile[1] as BIGINT) AS String),
                 cast(cast(rtt_std_percentile[2] as BIGINT) AS String),
                 cast(cast(rtt_std_percentile[3] as BIGINT) AS String),
                 cast(cast(rtt_std_percentile[4] as BIGINT) AS String))
from (
         select
                platform,
                version,
                type,
                cast(avg(cast(rtt_max as DECIMAL(16, 2))) as DECIMAL(16, 2))                       as avg_rtt_max,
                cast(avg(cast(rtt_min as DECIMAL(16, 2))) as DECIMAL(16, 2))                       as avg_rtt_min,
                cast(avg(cast(rtt_mean as DECIMAL(16, 2))) as DECIMAL(16, 2))                      as avg_rtt_mean,
                percentile_approx(cast(rtt_std as DECIMAL(16, 2)), array(0.1, 0.3, 0.5, 0.7, 0.9)) as rtt_std_percentile
         from (
                  select
                         platform,
                         version,
                         case
                             when conn_type = '0' then 'erizo'
                             when conn_type = '1' then 'p2p'
                             else 'other' end as type,
                         rtt_max,
                         rtt_min,
                         rtt_mean,
                         rtt_std
                  from ${APP}.dwd_rtc_android_du
                  where dt = '$do_date'
                    and version is not null
                    and over_num = success_num
                    and is_rtc = '1'
              ) t1
         group by platform,version, type grouping sets (platform,(platform,version),(platform,version,type))
     ) t2;"

ads_mobile_rtc_loss="
insert overwrite table ${APP}.ads_mobile_rtc_loss
select
       '$do_date',
       platform,
       if(version is null,'all',version),
       if(type is null,'all',type),
       avg_loss_max,
       avg_loss_mean,
       avg_loss_min,
       concat_ws(',', cast(cast(loss_std_percentile[0] as BIGINT) AS String),
                 cast(cast(loss_std_percentile[1] as BIGINT) AS String),
                 cast(cast(loss_std_percentile[2] as BIGINT) AS String),
                 cast(cast(loss_std_percentile[3] as BIGINT) AS String),
                 cast(cast(loss_std_percentile[4] as BIGINT) AS String))
from (
     select
            platform,
            version,
            type,
            cast(avg(loss_max) as DECIMAL(16, 2))             as avg_loss_max,
            cast(avg(loss_mean) as DECIMAL(16, 2))            as avg_loss_mean,
            cast(avg(loss_min) as DECIMAL(16, 2))             as avg_loss_min,
            percentile_approx(cast(loss_std as DECIMAL(16, 2)),
                              array(0.1, 0.3, 0.5, 0.7, 0.9)) as loss_std_percentile
     from (
              select
                     platform,
                     version,
                     case
                         when conn_type = '0' then 'erizo'
                         when conn_type = 1 then 'p2p'
                         else 'other' end as type,
                     loss_max,
                     loss_mean,
                     loss_min,
                     loss_std
              from ${APP}.dwd_rtc_android_du
              where dt = '$do_date'
                and version is not null
                and over_num = success_num
                and is_rtc = '1'
          ) t1
     group by platform,version, type grouping sets (platform,(platform,version),(platform,version,type))
 ) t2;"

ads_su_country_total="
set hive.support.quoted.identifiers=None;
insert overwrite table ${APP}.ads_su_country_total
select
    \`(r1)?+.+\`
from
(
    select
        *,row_number() over (partition by customer_type order by new_connect_count desc) r1
    from
    (
        select
            '$do_date' dt,
            scountry,
            customer_type,
            sum(if(handshake_success_connid is not null,1,0)) handshake_success_count,
            sum(if(authentication_success_connid is not null,1,0)) authentication_success_count,
            count(1) new_connect_count,
            cast(sum(if(handshake_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) handshake_success_rate,
            cast(sum(if(authentication_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) authentication_success_rate,
            if(cast(avg((\`out\`-\`in\`) / link_time)  AS DECIMAL(16,2) ) is null,0,cast(avg((\`out\`-\`in\`) / link_time)  AS DECIMAL(16,2) ) ) bitrate
        from
        (
            select connid,
                   scountry,
                   case
                       when enterprise='0' then 'customer'
                       when enterprise='1' then 'business'
                   else 'other' end customer_type
            from ${APP}.dwd_new_connect
            where dt='$do_date'
            and scountry is not null
            and scountry <> ''
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
        group by scountry,customer_type
    )t1
)t1
where r1<=30;"


ads_du_country_total="
set hive.support.quoted.identifiers=None;
insert overwrite table ${APP}.ads_du_country_total
select
    \`(r1)?+.+\`
from
(
    select
        *,row_number() over (partition by customer_type order by new_connect_count desc) r1
    from
    (
        select
            '$do_date' dt,
            dcountry,
            customer_type,
            sum(if(handshake_success_connid is not null,1,0)) handshake_success_count,
            sum(if(authentication_success_connid is not null,1,0)) authentication_success_count,
            count(1) new_connect_count,
            cast(sum(if(handshake_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) handshake_success_rate,
            cast(sum(if(authentication_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) authentication_success_rate,
            if(cast(avg((\`out\`-\`in\`) / link_time)  AS DECIMAL(16,2) ) is null,0,cast(avg((\`out\`-\`in\`) / link_time)  AS DECIMAL(16,2) ) ) bitrate
        from
        (
            select connid,
                   dcountry,
                   case
                       when enterprise='0' then 'customer'
                       when enterprise='1' then 'business'
                   else 'other' end customer_type
            from ${APP}.dwd_new_connect
            where dt='$do_date'
            and dcountry is not null
            and dcountry <> ''
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
        group by dcountry,customer_type
    )t1
)t1
where r1<=30;"

ads_tob_bandwidth_total="
with tmp as
(
    select
        get_json_object(line,'$.id') id,
        get_json_object(get_json_object(line,'$.turndata'),'$.company_id') company_id,
        get_json_object(get_json_object(line,'$.turndata'),'$.p2ps') + get_json_object(get_json_object(line,'$.turndata'),'$.tcps') s ,
        get_json_object(get_json_object(line,'$.turndata'),'$.p2pr')  + get_json_object(get_json_object(line,'$.turndata'),'$.tcpr') r,
        cast(get_json_object(get_json_object(line,'$.turndata'),'$.ConnTime') as bigint) ConnTime,
        cast(cast(get_json_object(line,'$.logtime')  as bigint) /1000 as bigint) logtime
    from ${APP}.ods_tuevent_log
    where dt='$do_date'
    and get_json_object(line,'$.events')='link_info'
    and get_json_object(line,'$.platform')='windows_master'
    and get_json_object(line,'$.version')>='2.0.1.2'
    and get_json_object(get_json_object(line,'$.turndata'),'$.company_id') is not null
    and get_json_object(get_json_object(line,'$.turndata'),'$.ConnTime')<>'0'
    union
    select
        id,
        company_id,
        cast(bytes_sent  as bigint) s,
        cast(bytes_received  as bigint) r,
        cast(cast(turndata_onlinetime as bigint)/1000 as bigint) ConnTime,
        cast(logtime /1000 as bigint) logtime
    from ${APP}.dwd_licode_ice
    where dt='$do_date'
    and version>='2.0.1.2'
    and platform in ('windows_slave','windows_master')
    and turndata_connection_type in ('erizo_pub','erizo_sub')
    and company_id is not null
    and cast(cast(turndata_onlinetime as bigint)/1000 as bigint) != 0
)
insert overwrite table ${APP}.ads_tob_bandwidth_total
select
    '$do_date',1,id,max(rkbyte),max(skbyte)
from
(
    select
        id,
        s,r,ConnTime,
        cast(cast(s as bigint)/1024/ConnTime as decimal(16,4)) skbyte,
        cast(cast(r as bigint)/1024/ConnTime as decimal(16,4)) rkbyte
    from tmp
)t1
group by id
union
select
    '$do_date',2,t3.company_id,max(in_rkbyte),max(in_skbyte)
from
(
    select
        t1.company_id,
        cast(if(!(t2.start_ts>t1.logtime or t2.logtime<t1.start_ts),t1.skbyte+t2.skbyte,t1.skbyte) as decimal(16,4)) in_skbyte,
        cast(if(!(t2.start_ts>t1.logtime or t2.logtime<t1.start_ts),t1.rkbyte+t2.rkbyte,t2.rkbyte) as decimal(16,4)) in_rkbyte
    from
    (
        select
            company_id,logtime-ConnTime start_ts,logtime,cast(s as bigint)/1024/ConnTime skbyte,cast(r as bigint)/1024/ConnTime rkbyte
        from tmp
    )t1
    inner join
    (
        select
            company_id,logtime-ConnTime start_ts,logtime,cast(s as bigint)/1024/ConnTime skbyte,cast(r as bigint)/1024/ConnTime rkbyte
        from tmp
    )t2
    on t1.company_id=t2.company_id
    where !(t1.logtime=t2.logtime and t1.start_ts=t2.start_ts and t1.rkbyte=t2.rkbyte and t1.skbyte=t2.skbyte)
)t3
group by t3.company_id;"

ads_vip_conn_duration="
insert overwrite table ${APP}.ads_vip_conn_duration
select
    '$do_date',
    vip,
    sum(link_time),
    cast(sum(link_time) / count(1) as bigint)
from
(
    select if(vip = '0' and dvip = '0',0,1) vip,link_time from ${APP}.dwd_session_success where dt='$do_date' and cast(link_time as bigint) > 0 and vip is not null  and dvip is not null
)t1
group by vip;"

ads_client_activesu="
insert overwrite table ${APP}.ads_client_activesu
    select
        '$do_date',
        client_type,
        7,
        count(1)
    from
    (
        select
            mac_id, client_type
        from
        (
            select
                mac_id, client_type ,\`if\`(client_type='ONTHER',9999,0) flag,count() over(partition by mac_id) c1
            from ${APP}.dws_su_action_daytotal
            where dt between date_sub('$do_date',6) and '$do_date'
            group by mac_id,client_type
        )t1
        where c1=1 or (c1>1 and flag=0)
    )t1
    group by client_type
    union all
    select
        '$do_date',
        client_type,
        30,
        count(1)
    from
    (
        select  
            mac_id, client_type
        from
        (
            select
                mac_id, client_type ,\`if\`(client_type='ONTHER',9999,0) flag,count() over(partition by mac_id) c1
            from ${APP}.dws_su_action_daytotal
            where dt between date_sub('$do_date',29) and '$do_date'
            group by mac_id,client_type
        )t1
        where c1=1 or (c1>1 and flag=0)
    )t1
    group by client_type;"
ads_client_activedu="
insert overwrite table ${APP}.ads_client_activedu
    select
        '$do_date',
        client_type,
        7,
        count(1)
    from
    (
        select
            mac_id, client_type
        from
        (
            select
                mac_id, client_type ,\`if\`(client_type='ONTHER',9999,0) flag,count() over(partition by mac_id) c1
            from ${APP}.dws_du_action_daytotal
            where dt between date_sub('$do_date',6) and '$do_date'
            group by mac_id,client_type
        )t1
        where c1=1 or (c1>1 and flag=0)
    )t1
    group by client_type
    union all
    select
        '$do_date',
        client_type,
        30,
        count(1)
    from
    (
        select
            mac_id, client_type
        from
        (
            select
                mac_id, client_type ,\`if\`(client_type='ONTHER',9999,0) flag,count() over(partition by mac_id) c1
            from ${APP}.dws_du_action_daytotal
            where dt between date_sub('$do_date',29) and '$do_date'
            group by mac_id,client_type
        )t1
        where c1=1 or (c1>1 and flag=0)
    )t1
    group by client_type;"

ads_operation_total="
insert overwrite table ${APP}.ads_operation_total
select
    '$do_date',1,suid,sclient
from
(
      select
          userid
      from
      (
          select
              userid,sum(total_fee) total_fee
          from ${APP}.dwd_tv_bill_order_detail
          where dt='$do_date'
          and product_id not in ('10','70','81','121','200','201','202')
          and status='c'
          group by userid
      )t1
      where total_fee < 30*100
      union
      select
          t2.userid
      from
      (
          select
              userid,sum(total_fee) total_fee,max(cast(id as bigint)) id
          from ${APP}.dwd_tv_bill_order_detail
          where dt='$do_date'
          and product_id not in ('10','70','81','121','200','201','202')
          and status='c'
          group by userid
      )t2
      inner join
      (
          select cast(id as bigint)id,end_time
          from ${APP}.dwd_tv_bill_order_detail
          where dt='$do_date'
          and product_id not in ('10','70','81','121','200','201','202')
          and status='c'
      )t3
      on t2.id=t3.id
      where total_fee > 30*100
      and end_time<(unix_timestamp(date_add('$do_date',1)) - 8*60*60)
)t2
inner join
(
    select
        suid,\`user\`,sclient
    from
    (
        select
            suid,\`user\`,sclient,sum(link_time) over(partition by \`user\`) link_time
        from
        (
            select
                connid,suid,\`user\`,sclient
            from ${APP}.dwd_new_connect
            where dt between date_sub('$do_date',13) and '$do_date'
            and enterprise='0'
            and sclient in ('ANDROID','IOS','PC')
            and svip='0'
            and \`user\` != '0'
            and \`user\` is not null
            and \`user\` != ''
        )t1
        inner join
        (
            select
                connid,sum(link_time) link_time
            from ${APP}.dwd_session_success
            where dt between date_sub('$do_date',13) and '$do_date'
            and enterprise='0'
            group by connid
        )t2
        on t1.connid = t2.connid
    )t3
    where link_time > 60 * 60
    group by suid,\`user\`,sclient
)t3
on t2.userid=t3.\`user\`
group by suid,sclient
union
SELECT '$do_date',2,suid,sclient
FROM (
  SELECT \`user\`,suid,sclient
  FROM ${APP}.dwd_new_connect
  WHERE dt between date_sub('$do_date',13) and '$do_date'
  AND ((scountry ='中国'AND scountry !='' AND dcountry !='中国'AND dcountry !='') OR (scountry !='中国'AND scountry !='' AND dcountry ='中国' AND dcountry !=''))
  AND enterprise='0'
  AND scountry is not null
  AND dcountry is not null
  AND sclient in ('ANDROID','IOS','PC','MAC')
  and \`user\` != '0'
  and \`user\` is not null
  and \`user\` != ''
  group by \`user\`,suid,sclient
)t1
inner JOIN
(
  select
    user_id
  from
  (
      SELECT user_id,collect_list(spu_id) spu_id
      FROM ${APP}.dwd_tv_function_config
      where dt = '$do_date'
      GROUP BY user_id
  )t0
  where  array_contains(spu_id,'7')=false
  union
  SELECT
    t0.user_id
  FROM (
      SELECT user_id,max(cast(id as bigint)) id
      FROM ${APP}.dwd_tv_function_config
      where dt = '$do_date'
      and spu_id='7'
      group by user_id
  )t0
  inner join
  (
      SELECT cast(id as bigint)id,end_time
      FROM ${APP}.dwd_tv_function_config
      where dt = '$do_date'
      and spu_id='7'
  )t1
  on t0.id=t1.id
  where end_time<(unix_timestamp(date_add('$do_date',1)) - 8*60*60)
)t2
on t1.\`user\` = t2.user_id
group by suid,sclient;"


ads_file_transport_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_file_transport_total
select
    '$do_date',
    labtype,
    count(1),
    sum(if(array_contains(a3,'1'),1,0)),
    sum(if(array_contains(a4,'1')  or array_contains(a4,'5') or  array_contains(a4,'4')  ,1,0)),
    sum(if(array_contains(a4,'1') or array_contains(a4,'5'),1,0))
from
(
    select
        n_id,labtype,
        collect_list(ec_su) a3,
        collect_list(ea_su) a4
    from
    (
        select
            n_id,labtype,ec_su,if(t10.ea_err is null ,'6',ea_err) ea_su
        from
        (
            select
                n_id,labtype,if(ec_su is null,'4',ec_su) ec_su
            from
            (
                select
                    connid n_id,labtype
                from ${APP}.dwd_newfile_connect
                where dt='$do_date'
                group by connid,labtype
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
        on t9.n_id=t10.id
    )t1
    group by n_id,labtype
)t2
group by labtype;"

ads_daas_serv_session_day="
insert overwrite table ${APP}.ads_daas_serv_session_day
select
    '$do_date',
    relay_server_name,
    sum(if(handshake_success_connid is not null,1,0)) c1,
    sum(if(authentication_success_connid is not null,1,0)) c1,
    count(1) c2,
    cast(sum(if(handshake_success_connid is not null,1,0)) /count(t1.connid) * 100 as decimal(16,2)),
    cast(sum(if(authentication_success_connid is not null,1,0)) /count(t1.connid) * 100 as decimal(16,2))
from
(	
    select
        connid, if(relay_server_name is  null or relay_server_name ='','other',relay_server_name) relay_server_name
    from
    (
        select
            connid,concat_ws('-',split(relay_server_name,'-')[0],split(relay_server_name,'-')[1],split(relay_server_name,'-')[3],split(relay_server_name,'-')[4]) relay_server_name
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        and labtype='daas'
        and resultmsg!='dbc'
    )t1
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
group by relay_server_name;
"

ads_daas_country_session_day="
insert overwrite table ${APP}.ads_daas_country_session_day
select
    '$do_date' dt,
    scountry,
    dcountry,
    sum(if(handshake_success_connid is not null,1,0)) handshake_success_count,
    sum(if(authentication_success_connid is not null,1,0)) authentication_success_count,
    count(1) new_connect_count,
    cast(sum(if(handshake_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) handshake_success_rate,
    cast(sum(if(authentication_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) authentication_success_rate
from
(
    select connid,
           if(scountry is null or scountry='','other',scountry) scountry,if(dcountry is null or dcountry='','other',dcountry) dcountry
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and labtype='daas'
    and resultmsg!='dbc'
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
        link_time
    from ${APP}.dwd_session_success
    where dt='$do_date'
    group by connid,link_time
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
group by scountry,dcountry
order by new_connect_count desc
limit 30;"

ads_daas_sprovince_session_day="
insert overwrite table ${APP}.ads_daas_sprovince_session_day
select
    '$do_date' dt,
    sprovince,
    sum(if(handshake_success_connid is not null,1,0)) handshake_success_count,
    sum(if(authentication_success_connid is not null,1,0)) authentication_success_count,
    count(1) new_connect_count,
    cast(sum(if(handshake_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) handshake_success_rate,
    cast(sum(if(authentication_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) authentication_success_rate,
       sum(cast(link_time as bigint))
from
(
    select connid,
           if(sprovince is null or sprovince ='' or scountry!='中国','other',sprovince) sprovince
    from ${APP}.dwd_new_connect
    where dt='$do_date' 
    and resultmsg!='dbc'
    and labtype='daas'
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
        link_time
    from ${APP}.dwd_session_success
    where dt='$do_date'
    group by connid,link_time
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
group by sprovince
order by new_connect_count desc;
"

ads_daas_dprovince_session_day="
insert overwrite table ${APP}.ads_daas_dprovince_session_day
select
    '$do_date' dt,
    case when resultmsg='gddxbmc' then '云电竞-电信广东'
         when resultmsg='btgz' then '云电竞-冰特广州'
         when resultmsg='qshc' then '云办公-启朔郑州1'
         when resultmsg='btjh' then '云电竞-冰特金华'
         when resultmsg='btty' then '云电竞-冰特太原'
         when resultmsg='btdy' then '云电竞-冰特德阳'
         when resultmsg='btyc' then '云电竞-冰特宜昌'
         when resultmsg='btjhb' then '云电竞-冰特金华2'
         when resultmsg='btgzb' then '云电竞-冰特广州2'
         when resultmsg='gzhc' then '云办公-电信广州1'
         when resultmsg='btjhc' then '云办公-冰特金华1'
         when resultmsg='gzcm' then '云办公-冰特广州2'
         when resultmsg='zuler' then 'zuler内部'
         when resultmsg='uculan' then '云办公-ucloud乌兰察布'
         when resultmsg='wangsugz' then '云办公-网宿广州'
         when resultmsg='btjhd' then '云办公-冰特金华2'
	 when resultmsg='clnb' then '云电竞-畅联宁波'
    else 'other' end,
    sum(if(handshake_success_connid is not null,1,0)) handshake_success_count,
    sum(if(authentication_success_connid is not null,1,0)) authentication_success_count,
    count(1) new_connect_count,
    cast(sum(if(handshake_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) handshake_success_rate,
    cast(sum(if(authentication_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) authentication_success_rate
from
(
    select connid,
           if(resultmsg is null or resultmsg='','other',resultmsg) resultmsg
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and labtype='daas'
    and resultmsg != 'dbc'
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
        link_time
    from ${APP}.dwd_session_success
    where dt='$do_date'
    group by connid,link_time
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
group by 
    case when resultmsg='gddxbmc' then '云电竞-电信广东'
         when resultmsg='btgz' then '云电竞-冰特广州'
         when resultmsg='qshc' then '云办公-启朔郑州1'
         when resultmsg='btjh' then '云电竞-冰特金华'
         when resultmsg='btty' then '云电竞-冰特太原'
         when resultmsg='btdy' then '云电竞-冰特德阳'
         when resultmsg='btyc' then '云电竞-冰特宜昌'
         when resultmsg='btjhb' then '云电竞-冰特金华2'
         when resultmsg='btgzb' then '云电竞-冰特广州2'
         when resultmsg='gzhc' then '云办公-电信广州1'
         when resultmsg='btjhc' then '云办公-冰特金华1'
         when resultmsg='gzcm' then '云办公-冰特广州2'
         when resultmsg='zuler' then 'zuler内部'
         when resultmsg='uculan' then '云办公-ucloud乌兰察布'
         when resultmsg='wangsugz' then '云办公-网宿广州'
         when resultmsg='btjhd' then '云办公-冰特金华2'
	 when resultmsg='clnb' then '云电竞-畅联宁波'
    else 'other' end
order by new_connect_count desc;"

ads_daas_isp_handshake="
insert overwrite table ${APP}.ads_daas_isp_handshake
select
    '$do_date',
    sisp,disp,
    sum(if(handshake_success_connid is not null,1,0)) c1,
    sum(if(authentication_success_connid is not null,1,0)) c1,
    count(1) c2,
    cast(sum(if(handshake_success_connid is not null,1,0)) /count(t1.connid) * 100 as decimal(16,2)),
    cast(sum(if(authentication_success_connid is not null,1,0)) /count(t1.connid) * 100 as decimal(16,2))
from
(
    select
        connid,
        case when sisp='中国联通' then '联通'
             when sisp='中国电信' then '电信'
             when sisp='中国移动' then '移动'
             else 'other' end sisp,
        case when disp='中国联通' then '联通'
                     when disp='中国电信' then '电信'
                     when disp='中国移动' then '移动'
                     else 'other' end disp
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and labtype='daas'
    and resultmsg!='dbc'
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
group by sisp,disp;"

ads_daas_handshake="
insert overwrite table ${APP}.ads_daas_handshake
select
       '$do_date',
       'presenter',
    sum(if(t2.connid is not null ,1,0)) c1,
    count(1) c2,
    cast(sum(if(t2.connid is not null , 1,0)) /count(t1.connid) * 100 as decimal(16,2))
from
(
    select connid
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and labtype='daas'
    and resultmsg!='dbc'
)t1
left join
(
    select
           connid
    from ${APP}.dwd_handshake_sd
    where dt='$do_date'
    and labtype='daas'
    and type='presenter'
    group by connid
)t2
on t1.connid=t2.connid
union
select
       '$do_date',
    'controller',
    sum(if(t2.connid is not null ,1,0)) c1,
    count(1) c2,
    cast(sum(if(t2.connid is not null , 1,0)) /count(t1.connid) * 100 as decimal(16,2))
from
(
    select connid
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and labtype='daas'
    and resultmsg!='dbc'
)t1
left join
(
    select
           connid
    from ${APP}.dwd_handshake_sd
    where dt='$do_date'
    and labtype='daas'
    and type='controller'
    group by connid
)t2
on t1.connid=t2.connid;"

ads_connect_limit_total="
insert overwrite table ${APP}.ads_connect_limit_total
select
    '$do_date',tmp_flag,count(distinct srcuser)
from
(
    select
        srcuser,tmp_flag
    from
    (
        select
            srcuser,message,
            case
                when cast(get_json_object(message,'$.count:') as bigint) = 100 and cast(get_json_object(message,'$.time:') as bigint) >= 60*3600 then 'count!time'
                when cast(get_json_object(message,'$.count:') as bigint) = 100 and cast(get_json_object(message,'$.time:') as bigint) < 60*3600 then 'count'
                when cast(get_json_object(message,'$.count:') as bigint) < 100 and cast(get_json_object(message,'$.time:') as bigint) >= 60*3600 then 'time'
                else flag end new_flag
        from ${APP}.dwd_connect_limit
        where dt='$do_date'
        and flag is not null
    )t1
    lateral view explode(split(new_flag,'!')) tmp as tmp_flag
)t1
group by tmp_flag;"

ads_daas_licode_handshake="
insert overwrite table ${APP}.ads_daas_licode_handshake
select
    '$do_date',flag,if(flag=1,all_pre,c1+all_pre),
    total,if(flag=1,cast(all_pre/total * 100 as decimal(16,2)),cast((c1+all_pre)/total * 100 as decimal(16,2)))
from
(
    select
        flag,c1,first_value(c1) over(order by flag) all_pre,sum(c1) over () total
    from
    (
        select
            if(ec_su is null,4,ec_su) flag,count(1) c1
        from
        (
            select
                t1.connid
            from
             (
                select connid
                from ${APP}.dwd_new_connect
                where dt='$do_date'
                and labtype='daas'
		and resultmsg!='dbc'
                 group by connid
             )t1
             inner join
             (
                 select connid
                 from ${APP}.dwd_authentication_success
                 where dt='$do_date'
                 group by connid
             )t2
             on t1.connid=t2.connid
        )t1
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
        )t2
        on t1.connid=t2.id
        group by ec_su
    )
    t3
)t4
where flag!=4;"

ads_daas_selling_time_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_daas_selling_time_total
select
    '$do_date',is_todesk,product_price_id,
    case
	when product_price_id in (1,105,120,126) then am * 3600
        when product_price_id in (2,83,56,66) then am * 86400
        when product_price_id in (3,82,57,67) then am * 604800
        when product_price_id in (4,81,58,68) then am * 2678400
        when product_price_id in (63,73,90,102,112,121,127) then am * 21600
        when product_price_id=7 then am * 8035200
        when product_price_id in (28,37,91,106,112,122,128) then am * 108000
        when product_price_id in (108,109,132,133) then am * 216000
        when product_price_id in (29,38,92,107,114,123,129) then am * 302400
        when product_price_id in (104,110,124,130) then am * 432000
        when product_price_id in (30,39,93,103,111,125,131) then am * 1296000
    else 0 end selling_time
from
(
    select
        is_todesk, product_price_id, sum(amount) am
    from
    (
        select
            if(t2.userid is not null,1,0) is_todesk,product_price_id,amount
        from
        (
        select
               userid,product_price_id,amount
            from ${APP}.ods_daas_bill_order_detail 
            where dt='$do_date'
            and env=2 and status=2 and total_fee>0 and order_type>0
            and product_id !=1000
            and unix_timestamp(create_time)-28800>=unix_timestamp('$do_date 00:00:00')-28800
            and unix_timestamp(create_time)-28800<unix_timestamp(date_add('$do_date',1))-28800
        )t1
        left join
        (
            select
                userid
            from ${APP}.ods_daas_tv_todesk_user
            where dt='$do_date'
            group by userid
        )t2
        on t1.userid=t2.userid
    )t3
    group by is_todesk,product_price_id
)t4;"

ads_daas_used_time_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.vectorized.execution.enabled=false;
insert overwrite table ${APP}.ads_daas_used_time_total
select *
from(
    select
        '$do_date',1,flag,sum(time_total) time_total
    from
    (
        select
            case
                when connected_time>=dt_start and connected_time <dt_end
                     and disconnected_time>=connected_time then 1
                when connected_time<dt_start
                     and disconnected_time>=dt_start and disconnected_time <dt_end then 2
                when connected_time>=dt_start and connected_time <dt_end
                     and disconnected_time<connected_time    then 3
                when connected_time<dt_start
                     and disconnected_time<connected_time   then 4
            else 0 end flag,
            case
                when connected_time>=dt_start and connected_time <dt_end
                     and disconnected_time>=connected_time then disconnected_time-connected_time
                when connected_time<dt_start
                     and disconnected_time>=dt_start and disconnected_time <dt_end then disconnected_time-(dt_start)
                when connected_time>=dt_start and connected_time <dt_end
                     and disconnected_time<connected_time   then dt_end-connected_time
                when connected_time<dt_start
                     and disconnected_time<connected_time   then dt_end-(dt_start)
            else 0 end time_total
        from
        (
            select
                virtual_desktop_id,
                unix_timestamp(split(connected_time,'\\\\.')[0]) -28800 connected_time,
                unix_timestamp(split(disconnected_time,'\\\\.')[0]) -28800 disconnected_time,
                unix_timestamp('$do_date 00:00:00')-28800 dt_start,
                unix_timestamp(date_add('$do_date',1))-28800 dt_end
            from ${APP}.dwd_daas_tv_machine_connection
            where dt='$do_date'
            AND group_name = '默认云桌面组'
        )t1
        left join
        (
            select
                virtual_desktop_id
            from ${APP}.dwd_daas_tv_machine_powerchange
            where dt='$do_date'
            group by virtual_desktop_id
        )t2
        on t1.virtual_desktop_id=t2.virtual_desktop_id
        WHERE t2.virtual_desktop_id IS NULL
    )t3
    group by flag
) t1
where flag!='0'
union
select * from(
select
'$do_date',8, flag,sum(time_total) time_total
from
(
    select
        case
            when poweron_time>=dt_start and poweron_time <dt_end
                 and poweroff_time>=poweron_time then 1
            when poweron_time>dt_total and poweron_time <dt_start
                 and poweroff_time>=dt_start and poweroff_time <dt_end then 2
            when poweron_time>=dt_start and poweron_time <dt_end
                 and poweroff_time is null   then 3
            when poweron_time>dt_total and poweron_time <dt_start
                 and poweroff_time is null   then 4
        else 0 end flag,
        case
            when poweron_time>=dt_start and poweron_time <dt_end
                 and poweroff_time>=poweron_time then poweroff_time-poweron_time
            when poweron_time>dt_total and poweron_time <dt_start
                 and poweroff_time>=dt_start and poweroff_time <dt_end then poweroff_time-(dt_start)
            when poweron_time>=dt_start and poweron_time <dt_end
                 and poweroff_time is null   then dt_end-poweron_time
            when poweron_time>dt_total and poweron_time <dt_start
                 and poweroff_time is null   then dt_end-(dt_start)
        else 0 end time_total
    from
    (
        select
            unix_timestamp(split(poweron_time,'\\\\.')[0]) -28800 poweron_time,
            unix_timestamp(split(poweroff_time,'\\\\.')[0]) -28800 poweroff_time,
            unix_timestamp('$do_date 00:00:00')-28800 dt_start,
            unix_timestamp(date_add('$do_date',1))-28800 dt_end,
            unix_timestamp('$do_date 00:00:00')-28800 dt_total
        from ${APP}.dwd_daas_tv_machine_powerchange
        where dt='$do_date'
    )tt
)t1
group by flag )t1 where flag!=0;"


ads_daas_user_retention="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_daas_user_retention
select
    '$do_date',0,date_sub('$do_date',1),1,sum(if(t2.\`user\` is not null,1,0)),count(1),cast(sum(if(t2.\`user\` is not null,1,0))/count(1) * 100 as decimal(16,2))
from
(
    select
        id
    from ${APP}.ods_daas_tv_user
    where dt='$do_date'
    and date_format(from_utc_timestamp(regdate*1000,'Asia/Shanghai'),'yyyy-MM-dd')=date_sub('$do_date',1)
)t1
left join
(
    select
        \`user\`
    from ${APP}.dwd_new_connect
    where dt = '$do_date'
    and labtype='daas'
    group by \`user\`
)t2
on t1.id=t2.\`user\`
union
select
    '$do_date',0,date_sub('$do_date',7),7,sum(if(t2.\`user\` is not null,1,0)),count(1),cast(sum(if(t2.\`user\` is not null,1,0))/count(1) * 100 as decimal(16,2))
from
(
    select
        id
    from ${APP}.ods_daas_tv_user
    where dt='$do_date'
    and date_format(from_utc_timestamp(regdate*1000,'Asia/Shanghai'),'yyyy-MM-dd')=date_sub('$do_date',7)
)t1
left join
(
    select
        \`user\`
    from ${APP}.dwd_new_connect
    where dt = '$do_date'
    and labtype='daas'
    group by \`user\`
)t2
on t1.id=t2.\`user\`
union
select
    '$do_date',0,date_sub('$do_date',15),15,sum(if(t2.\`user\` is not null,1,0)),count(1),cast(sum(if(t2.\`user\` is not null,1,0))/count(1) * 100 as decimal(16,2))
from
(
    select
        id
    from ${APP}.ods_daas_tv_user
    where dt='$do_date'
    and date_format(from_utc_timestamp(regdate*1000,'Asia/Shanghai'),'yyyy-MM-dd')=date_sub('$do_date',15)
)t1
left join
(
    select
        \`user\`
    from ${APP}.dwd_new_connect
    where dt = '$do_date'
    and labtype='daas'
    group by \`user\`
)t2
on t1.id=t2.\`user\`
union
select
    '$do_date',0,date_sub('$do_date',30),30,sum(if(t2.\`user\` is not null,1,0)),count(1),cast(sum(if(t2.\`user\` is not null,1,0))/count(1) * 100 as decimal(16,2))
from
(
    select
        id
    from ${APP}.ods_daas_tv_user
    where dt='$do_date'
    and date_format(from_utc_timestamp(regdate*1000,'Asia/Shanghai'),'yyyy-MM-dd')=date_sub('$do_date',30)
)t1
left join
(
    select
        \`user\`
    from ${APP}.dwd_new_connect
    where dt = '$do_date'
    and labtype='daas'
    group by \`user\`
)t2
on t1.id=t2.\`user\`;
insert overwrite table ${APP}.ads_daas_user_retention
select * from ${APP}.ads_daas_user_retention
union
select
    t1.dt,1,cast(date_sub('$do_date',1) as string),1,retention_user_count,login_cnt,cast(100 * retention_user_count/login_cnt as decimal(16,2))
from

(
    select
        dt, flag, register_date, retention_day, retention_user_count, retention_user_rate
    from ${APP}.ads_daas_user_retention
    where dt='$do_date'
)t1
inner join
(
    select
        dt, daas_cnt+ todesk_login_cnt login_cnt
    from ${APP}.ads_daas_user_distributed_total
    where  dt=date_sub('$do_date',1)
)t2
on t1.register_date=t2.dt
union
select
    t1.dt,1,cast(date_sub('$do_date',7) as string),7,retention_user_count,login_cnt,cast(100 * retention_user_count/login_cnt as decimal(16,2))
from

(
    select
        dt, flag, register_date, retention_day, retention_user_count, retention_user_rate
    from ${APP}.ads_daas_user_retention
    where dt='$do_date'
)t1
inner join
(
    select
        dt, daas_cnt+ todesk_login_cnt login_cnt
    from ${APP}.ads_daas_user_distributed_total
    where  dt=date_sub('$do_date',7)
)t2
on t1.register_date=t2.dt
union
select
    t1.dt,1,cast(date_sub('$do_date',15) as string),15,retention_user_count,login_cnt,cast(100 * retention_user_count/login_cnt as decimal(16,2))
from

(
    select
        dt, flag, register_date, retention_day, retention_user_count, retention_user_rate
    from ${APP}.ads_daas_user_retention
    where dt='$do_date'
)t1
inner join
(
    select
        dt, daas_cnt+ todesk_login_cnt login_cnt
    from ${APP}.ads_daas_user_distributed_total
    where  dt=date_sub('$do_date',15)
)t2
on t1.register_date=t2.dt
union
select
    t1.dt,1,cast(date_sub('$do_date',30) as string),30,retention_user_count,login_cnt,cast(100 * retention_user_count/login_cnt as decimal(16,2))
from

(
    select
        dt, flag, register_date, retention_day, retention_user_count, retention_user_rate
    from ${APP}.ads_daas_user_retention
    where dt='$do_date'
)t1
inner join
(
    select
        dt, daas_cnt+ todesk_login_cnt login_cnt
    from ${APP}.ads_daas_user_distributed_total
    where  dt=date_sub('$do_date',30)
)t2
on t1.register_date=t2.dt;"

ads_daas_pay_total="
insert overwrite table ${APP}.ads_daas_pay_total
select
    t1.dt, 1_reg, 7_reg, 30_reg,total_reg,
    1_order, 7_order, 30_order, total_order,
    old_order
from
(
    select
    '$do_date' dt,
    sum(if(regdate>=unix_timestamp('$do_date 00:00:00')-28800 and regdate<unix_timestamp(date_add('$do_date',1))-28800,1,0)) 1_reg,
    sum(if(regdate>=unix_timestamp(date_sub('$do_date',6))-28800 and regdate<unix_timestamp(date_add('$do_date',1))-28800,1,0)) 7_reg,
    sum(if(regdate>=unix_timestamp(date_sub('$do_date',29))-28800 and regdate<unix_timestamp(date_add('$do_date',1))-28800,1,0)) 30_reg,
    count(1) total_reg
    from ${APP}.ods_daas_tv_user
    where dt='$do_date'
)t1
left join
(
    select
        t1.dt,1_order,7_order,30_order,total_order
    from
     (
        select
            '$do_date' dt,
            count(distinct userid) 1_order
        from ${APP}.ods_daas_bill_order_detail
        where dt='$do_date'
        and env=2 and status=2 and total_fee>0 and order_type>0
        and unix_timestamp(create_time)-28800>=unix_timestamp('$do_date 00:00:00')-28800
        and unix_timestamp(create_time)-28800<unix_timestamp(date_add('$do_date',1))-28800
     )t1
     left join
     (
         select
            '$do_date' dt,
            count(distinct userid) 7_order
        from ${APP}.ods_daas_bill_order_detail
        where dt='$do_date'
        and env=2 and status=2 and total_fee>0 and order_type>0
        and unix_timestamp(create_time)-28800>=unix_timestamp(date_sub('$do_date',6))-28800
        and unix_timestamp(create_time)-28800<unix_timestamp(date_add('$do_date',1))-28800
     )t2
     on t1.dt=t2.dt
     left join
     (
         select
            '$do_date' dt,
            count(distinct userid) 30_order
        from ${APP}.ods_daas_bill_order_detail
        where dt='$do_date'
        and env=2 and status=2 and total_fee>0 and order_type>0
        and unix_timestamp(create_time)-28800>=unix_timestamp(date_sub('$do_date',29))-28800
        and unix_timestamp(create_time)-28800<unix_timestamp(date_add('$do_date',1))-28800
     )t3
     on t1.dt=t3.dt
     left join
     (
         select
            '$do_date' dt,
            count(distinct userid) total_order
        from ${APP}.ods_daas_bill_order_detail
        where dt='$do_date'
        and env=2 and status=2 and total_fee>0 and order_type>0
     )t4
     on t1.dt=t4.dt
)t2
on t1.dt=t2.dt
left join
(
    select
        '$do_date' dt,
        count(1) old_order
    from
    (
        select
            userid
         from ${APP}.ods_daas_bill_order_detail
         where dt='$do_date'
         and env=2 and status=2 and total_fee>0 and order_type>0
         and product_id=1 and product_price_id in (1,2,3,4,5,6,7,8)
         and unix_timestamp(create_time)-28800>=unix_timestamp('$do_date 00:00:00')-28800
         and unix_timestamp(create_time)-28800<unix_timestamp(date_add('$do_date',1))-28800
         group by userid
    )t1
    left join
    (
         select userid from ${APP}.ods_daas_bill_order_detail
         where dt='$do_date'
         and env=2 and status=2 and total_fee>0 and order_type>0 and unix_timestamp(create_time)-28800 < unix_timestamp('$do_date 00:00:00')-28800
         group by userid
    )t2
    on t1.userid=t2.userid
    where t2.userid  is not null
)t3
on t1.dt=t3.dt;"

ads_daas_order_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_daas_order_total
select
    '$do_date',is_todesk,0,product_price_id,count(1)
from
(
    select
        if(t2.userid is not null,1,0) is_todesk,product_price_id
    from
    (
	select
            t1.userid, product_price_id
        from
        (
            select
                userid,product_price_id
            from ${APP}.ods_daas_bill_order_detail
            where dt='$do_date'
            and env=2 and status=2 and total_fee>0 and order_type>0
            and product_id !=1000
            and unix_timestamp(create_time)-28800>=unix_timestamp('$do_date 00:00:00')-28800
            and unix_timestamp(create_time)-28800<unix_timestamp(date_add('$do_date',1))-28800
        )t1
        left join
        (
            select userid from ${APP}.ods_daas_bill_order_detail
             where dt='$do_date'
             and env=2 and status=2 and total_fee>0 and order_type>0 and product_id !=1000 and unix_timestamp(create_time)-28800 < unix_timestamp('$do_date 00:00:00')-28800
             group by userid
        )t2
        on t1.userid=t2.userid
        where t2.userid is  null
    )t1
    left join
    (
        select
            userid
        from ${APP}.ods_daas_tv_todesk_user
        where dt='$do_date'
        group by userid
    )t2
    on t1.userid=t2.userid
)t3
group by is_todesk,product_price_id
union
select
    '$do_date',is_todesk,1,product_price_id,count(1)
from
(
    select
        if(t2.userid is not null,1,0) is_todesk,product_price_id
    from
    (
	select
            t1.userid, product_price_id
        from
        (
            select
                userid,product_price_id
            from ${APP}.ods_daas_bill_order_detail
            where dt='$do_date'
            and env=2 and status=2 and total_fee>0 and order_type>0
            and product_id !=1000
            and unix_timestamp(create_time)-28800>=unix_timestamp('$do_date 00:00:00')-28800
            and unix_timestamp(create_time)-28800<unix_timestamp(date_add('$do_date',1))-28800
        )t1
        left join
        (
            select userid from ${APP}.ods_daas_bill_order_detail
             where dt='$do_date'
             and env=2 and status=2 and total_fee>0 and order_type>0 and product_id !=1000 and unix_timestamp(create_time)-28800 < unix_timestamp('$do_date 00:00:00')-28800
             group by userid
        )t2
        on t1.userid=t2.userid
        where t2.userid is not null
    )t1
    left join
    (
        select
            userid
        from ${APP}.ods_daas_tv_todesk_user
        where dt='$do_date'
        group by userid
    )t2
    on t1.userid=t2.userid
)t3
group by is_todesk,product_price_id;"

ads_tob_company_total="
insert overwrite table ${APP}.ads_tob_company_total
select
    '$do_date',1 flag,bid, c1,'',link_time
from
(
    select
        bid,count(1) c1,sum(link_time) link_time
    from
    (
        select
            bid,connid
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        and enterprise='1'
        and bid is not null
        and bid != '0'
    )t1
    inner join
    (
       select
            connid,link_time
       from ${APP}.dwd_session_success
        where dt='$do_date'
        group by connid,link_time
    )t2
    on t1.connid=t2.connid
    group by bid
)t1
where c1 >=100
union all
select
    '$do_date',2,company_id,c1,concat_ws(',',city_list),0
from
(
    select
        company_id,count(1) c1,collect_set(pr) city_list
    from
    (
        select
            company_id,split(pr,'市')[0] pr
        from
         (
            select company_id,
                   case
                    when substr(city,0,3)in ('广东省','江苏省','浙江省','福建省','山东省','河南省','四川省','河北省','辽宁省','吉林省','湖南省','安徽省','湖北省','江西省','陕西省','山西省','贵州省','云南省','甘肃省','海南省','青海省')
                         then split(city,'省')[1]
                    when substr(city,0,4)='黑龙江省'
                         then split(city,'省')[1]
                    when split(city,'区')[0] in ('宁夏回族自治','西藏自治','新疆维吾尔自治','广西壮族自治','内蒙古自治')
                         then split(city,'区')[1]
                    when substr(city,3,2) in ('台湾','香港')
                         then substr(city,3,2)
                    when city = '澳门特别行政区'
                         then '澳门'
                    else city end pr
            from ${APP}.dwd_user_login
            where dt='$do_date'
            and \`user\` is not null
            and enterprise='1'
            and company_id is not null
            and company_id != '0'
            and city is not null
            and city != ''
         )t0
        where pr !=''
        group by company_id,pr
    )t1
    group by company_id
)t1
where c1>=5
union all
select
    '$do_date',3,sip,c1,concat_ws(',',company_id_list),0
from
(
    select
        sip,count(1) c1,collect_set(company_id) company_id_list
    from
    (
        select sip,company_id
        from ${APP}.dwd_user_login
        where dt='$do_date'
        and \`user\` is not null
        and enterprise='1'
        and company_id is not null
        and company_id != '0'
        and sip is not  null
        and sip != ''
        group by sip,company_id
    )t1
    group by sip
)t1
where c1>=2
union all
select
    '$do_date',4,bid,c1,concat_ws(',',scity_list),0
from
(
    select
        bid,count(1) c1,collect_set(scity) scity_list
    from
    (
        select
            bid,split(scity,'市')[0]  scity
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        and enterprise='1'
        and bid is not null
        and bid != '0'
        and scity is not null
        and scity != ''
        group by bid,scity
    )t1
    group by bid
)t1
where c1>=5
union all
select
    '$do_date',5,bid,c1,concat_ws(',',dcity_list),0
from
(
    select
        bid,count(1) c1,collect_set(dcity) dcity_list
    from
    (
        select
            bid,split(dcity,'市')[0]  dcity
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        and enterprise='1'
        and bid is not null
        and bid != '0'
        and dcity is not null
        and dcity != ''
        group by bid,dcity
    )t1
    group by bid
)t1
where c1>=5;"

ads_todesk_province_total="
insert overwrite table  ${APP}.ads_todesk_province_total
select
    '$do_date',0,pr,count(1)
from
(
    select
        *
    from
    (
        select
            \`user\`, pr
        from
        (
            select
                \`user\`,
                case
                    when substr(city,0,2)in ('广东','江苏','浙江','福建','山东','河南','四川','河北','辽宁','吉林','湖南','安徽','湖北','江西','陕西','山西','贵州','云南','甘肃','海南','青海','上海','北京','重庆','天津','宁夏','西藏','新疆','广西','澳门')
                         then substr(city,0,2)
                    when substr(city,0,3) in ('黑龙江','内蒙古')
                         then substr(city,0,3)
                    when substr(city,3,2) in ('台湾','香港')
                         then substr(city,3,2)
                    else '' end pr
            from ${APP}.dwd_user_login
            where dt='$do_date'
            and enterprise='0'
        )t1
        where pr !=''
        group by pr,\`user\`
    )t1
    inner join
    (
        select
            id
        from ${APP}.dim_user_info
        where dt='$do_date'
        and create_time>=unix_timestamp('$do_date 00:00:00')-28800
        and create_time<unix_timestamp(date_add('$do_date',1))-28800
    )t2
    on t1.\`user\`=t2.id
)t2
group by pr;"

ads_connect_active_total="
insert overwrite table ${APP}.ads_connect_active_total
select
    '$do_date',0,client,count(1)
from ${APP}.dws_mac_connect_active
where dt='$do_date'
group by client
union
select '$do_date',1,pl,count(1)
from
 (
    select suid,max(client) pl
    from ${APP}.dws_mac_connect_active
    where dt between trunc('$do_date','MM') and '$do_date'
    group by suid
 )t1
group by pl;
"

ads_user_limit_total="
insert overwrite table ${APP}.ads_user_limit_total
select
    '$do_date',reason,
    count(1),sum(c1),sum(if(t2.userid is not null,1,0)),
    cast(sum(if(t2.userid is not null,1,0))/count(1) *100 as decimal(16,2)),sum(fee),
    cast(sum(fee)/sum(if(t2.userid is not null,1,0)) as decimal(16,2))
from
(
    select
        srcuser,reason,
        case when reason='rps' then 3
             when reason in ('tps','countAndHour','GreenLimit','VirtualPhone','LoginCount','randomDisconnect','tuPaoYunBan') then 1 else 0 end flag,c1
    from
     (
        select  srcuser,flag reason,count(1) c1
        from ${APP}.dwd_connect_limit
        where dt='$do_date'
        and srcuser != ''
        and srcuser is not null
        and flag in ('tps','rps','countAndHour','GreenLimit','VirtualPhone','LoginCount','randomDisconnect','tuPaoYunBan')
        group by flag, srcuser
     )t1
    union
    select srcuser,'sline',
           2,count(1) c1
    from ${APP}.dwd_connect_limit_sline
    where dt='$do_date'
    and srcuser != ''
    and srcuser is not null
    group by srcuser
)t1
left join
(
    select
        userid,
        case when spu_id in ('2','100','101') then 1
             when spu_id ='7' then 2
             when spu_id ='5' then 3 else 0 end flag,
        sum(total_fee) fee
    from
    (
        select
            userid,product_id,total_fee
        from ${APP}.dwd_tv_bill_order_detail
        where dt='$do_date'
        and status='c'
        and cast(total_fee as bigint)>1
        and substr(create_time,0,10)='$do_date'
    )t1
    inner join
    (
        select
            id, spu_id
        from ${APP}.dwd_tv_product
        where dt='$do_date'
        and spu_id in ('2','100','101','7','5')
    )t2
    on t1.product_id=t2.id
    group by userid,case when spu_id in ('2','100','101') then 1
             when spu_id ='7' then 2
             when spu_id ='5' then 3 else 0 end
)t2
on t1.srcuser=t2.userid and t1.flag=t2.flag
group by reason;"

ads_platform_detail_total="
insert overwrite table ${APP}.ads_platform_detail_total
select
    dt, labtype,client_platform, host_platform, client_version, host_version, client_network, host_network, sprovince, dprovince, conn_type, host_codec_name, client_codec_name, gear, d3d, track_count, active_switch, refresh_rate, const_rate, rapid_refresh, true_coler, if(resultmsg is not null,resultmsg,-1), if(trails_user is not null,trails_user,-1), if(local_proto is not null,local_proto,-1),'-1','-1','-1','-1','-1',
    first_frame_time, concat_ws(',', cast(cast(first_frame_time_pct[0] as decimal(16,2)) AS String),
                 cast(cast(first_frame_time_pct[1] as decimal(16,2)) AS String),
                 cast(cast(first_frame_time_pct[2] as decimal(16,2)) AS String),
                 cast(cast(first_frame_time_pct[3] as decimal(16,2)) AS String),
                 cast(cast(first_frame_time_pct[4] as decimal(16,2)) AS String)),
    count_0_500ms,count_500_1000ms,count_1_3s, count_3_5s, count_5_10s, count_10s_upper, objective_mark, subjective_mark, host_connect_signaling_duration, host_publish_signaling_duration, host_ice_success_duration,
    avg_fst1, avg_fst2, avg_fst3, avg_fst4, avg_fst5, client_connect_signaling_duration, client_publish_signaling_duration, client_ice_success_duration,
    client_fst1, client_fst2, client_fst3, client_fst4, client_fst5,
    tar_rate_mean,concat_ws(',', cast(cast(tar_rate_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(tar_rate_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(tar_rate_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(tar_rate_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(tar_rate_mean_pct[4] as decimal(16,2)) AS String)),
    act_rate_mean,concat_ws(',', cast(cast(act_rate_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(act_rate_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(act_rate_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(act_rate_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(act_rate_mean_pct[4] as decimal(16,2)) AS String)),
    send_rate_mean,concat_ws(',', cast(cast(send_rate_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(send_rate_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(send_rate_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(send_rate_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(send_rate_mean_pct[4] as decimal(16,2)) AS String)),
    rcvd_rate_mean,concat_ws(',', cast(cast(rcvd_rate_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(rcvd_rate_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(rcvd_rate_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(rcvd_rate_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(rcvd_rate_mean_pct[4] as decimal(16,2)) AS String)),
    rtx_rate_mean, fec_rate_mean, rtx_recover_rate, fec_recover_rate,
    cap_fr_mean, concat_ws(',', cast(cast(cap_fr_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(cap_fr_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(cap_fr_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(cap_fr_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(cap_fr_mean_pct[4] as decimal(16,2)) AS String)),
    tar_fr_mean, concat_ws(',', cast(cast(tar_fr_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(tar_fr_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(tar_fr_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(tar_fr_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(tar_fr_mean_pct[4] as decimal(16,2)) AS String)),
    send_fr_mean, concat_ws(',', cast(cast(send_fr_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(send_fr_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(send_fr_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(send_fr_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(send_fr_mean_pct[4] as decimal(16,2)) AS String)),
    rcvd_fr_mean, concat_ws(',', cast(cast(rcvd_fr_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(rcvd_fr_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(rcvd_fr_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(rcvd_fr_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(rcvd_fr_mean_pct[4] as decimal(16,2)) AS String)),
    dec_fr_mean, concat_ws(',', cast(cast(dec_fr_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(dec_fr_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(dec_fr_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(dec_fr_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(dec_fr_mean_pct[4] as decimal(16,2)) AS String)),
    send_render_fr_mean, concat_ws(',', cast(cast(send_render_fr_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(send_render_fr_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(send_render_fr_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(send_render_fr_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(send_render_fr_mean_pct[4] as decimal(16,2)) AS String)),
    act_render_fr_mean,
    es_bwe_mean, concat_ws(',', cast(cast(es_bwe_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(es_bwe_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(es_bwe_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(es_bwe_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(es_bwe_mean_pct[4] as decimal(16,2)) AS String)),
    bwe_mean,concat_ws(',', cast(cast(bwe_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(bwe_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(bwe_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(bwe_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(bwe_mean_pct[4] as decimal(16,2)) AS String)),
    rtt_mean,concat_ws(',', cast(cast(rtt_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(rtt_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(rtt_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(rtt_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(rtt_mean_pct[4] as decimal(16,2)) AS String)),
    loss_mean,concat_ws(',', cast(cast(loss_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(loss_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(loss_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(loss_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(loss_mean_pct[4] as decimal(16,2)) AS String)),
    e2e_delay, stuck_ratio, avg_render_stuck_ratio, render_duration_pct_90, render_duration_pct_99, render_duration_pct_999, loss_frame_rate,
    qp_mean,concat_ws(',', cast(cast(qp_mean_pct[0] as decimal(16,2)) AS String),
                 cast(cast(qp_mean_pct[1] as decimal(16,2)) AS String),
                 cast(cast(qp_mean_pct[2] as decimal(16,2)) AS String),
                 cast(cast(qp_mean_pct[3] as decimal(16,2)) AS String),
                 cast(cast(qp_mean_pct[4] as decimal(16,2)) AS String)),
    c1,vcap_detail,venc_detail,vpac_detail,vrec_detail,vjit_detail,vdec_detail,vstk_detail,vrtc_detail,vqp_detail,bur_detail,next_delay_detail,frm_recv_rate_detail,frm_drain_rate_detail,
-1,-1,'','','',-1
from
(
    select
        '$do_date' dt,
        if(labtype is null,'-1',labtype) labtype,
        if(client_platform is null,'-1',client_platform) client_platform,
        if(host_platform is null,'-1',host_platform) host_platform,
        if(client_version is null,'-1',client_version) client_version,
        if(host_version is null,'-1',host_version) host_version,
        if(client_network is null,'-1',client_network) client_network,
        if(host_network is null,'-1',host_network) host_network,
        if(sprovince is null,'-1',sprovince) sprovince,
        if(dprovince is null,'-1',dprovince) dprovince,
        if(conn_type is null,'-1',conn_type) conn_type,
        if(host_codec_name is null,'-1',host_codec_name) host_codec_name,
        if(client_codec_name is null,'-1',client_codec_name) client_codec_name,
        '-1' gear,'-1' d3d,track_count,'-1' active_switch,'-1' refresh_rate,'-1' const_rate,'-1' rapid_refresh,'-1' true_coler,
        resultmsg,trails_user,local_proto,
        cast(sum(if(first_frame_time between 0 and 25000, first_frame_time ,0)) / sum(if(first_frame_time  between 0 and 25000,1,0)) as decimal(16,2)) first_frame_time,
        percentile_approx(cast(first_frame_time as decimal(16,2)),
                                      array( 0.3, 0.5, 0.7, 0.9)) first_frame_time_pct,
	sum(if(first_frame_time >=0 and first_frame_time<5*100,1,0)) count_0_500ms,
        sum(if(first_frame_time >=5*100 and first_frame_time<1*1000,1,0)) count_500_1000ms,
        sum(if(first_frame_time >=1*1000 and first_frame_time<3*1000,1,0)) count_1_3s,
        sum(if(first_frame_time >=3*1000 and first_frame_time<5*1000,1,0)) count_3_5s,
        sum(if(first_frame_time >=5*1000 and first_frame_time<10*1000,1,0)) count_5_10s,
        sum(if(first_frame_time >=10*1000,1,0)) count_10s_upper,-1 objective_mark,-1 subjective_mark,
    cast((sum(if(if(split(host_dur[0],'_')[0]='publish',cast(split(host_dur[1],'_')[2] as bigint),cast(split(host_dur[0],'_')[2] as bigint)) between 0 and 30000, if(split(host_dur[0],'_')[0]='publish',cast(split(host_dur[1],'_')[2] as bigint),cast(split(host_dur[0],'_')[2] as bigint)) ,0)) / sum(if(if(split(host_dur[0],'_')[0]='publish',cast(split(host_dur[1],'_')[2] as bigint),cast(split(host_dur[0],'_')[2] as bigint))  between 0 and 30000,1,0))) as decimal(16,2)) host_connect_signaling_duration,
        cast((sum(if(if(split(host_dur[0],'_')[0]='publish',cast(split(host_dur[0],'_')[1] as bigint),cast(split(host_dur[1],'_')[1] as bigint)) between 0 and 30000, if(split(host_dur[0],'_')[0]='publish',cast(split(host_dur[0],'_')[1] as bigint),cast(split(host_dur[1],'_')[1] as bigint)) ,0)) / sum(if(if(split(host_dur[0],'_')[0]='publish',cast(split(host_dur[0],'_')[1] as bigint),cast(split(host_dur[1],'_')[1] as bigint))  between 0 and 30000,1,0))) as decimal(16,2)) host_publish_signaling_duration,
        -1 host_ice_success_duration,
        cast(sum(if(dufst1 between 0 and 30000, dufst1 ,0)) / sum(if(dufst1  between 0 and 30000,1,0)) as decimal(16,2))  avg_fst1,
        cast(sum(if(dufst2 between 0 and 30000, dufst2 ,0)) / sum(if(dufst2  between 0 and 30000,1,0)) as decimal(16,2))  avg_fst2,
        cast(sum(if(dufst3 between 0 and 30000, dufst3 ,0)) / sum(if(dufst3  between 0 and 30000,1,0)) as decimal(16,2))  avg_fst3,
        cast(sum(if(dufst4 between 0 and 30000, dufst4 ,0)) / sum(if(dufst4  between 0 and 30000,1,0)) as decimal(16,2))  avg_fst4,
        cast(sum(if(dufst5 between 0 and 30000, dufst5 ,0)) / sum(if(dufst5  between 0 and 30000,1,0)) as decimal(16,2))  avg_fst5,
        cast((sum(if(if(split(host_dur[0],'_')[0]='subscribe',cast(split(host_dur[1],'_')[2] as bigint),cast(split(host_dur[0],'_')[2] as bigint)) between 0 and 30000, if(split(host_dur[0],'_')[0]='subscribe',cast(split(host_dur[1],'_')[2] as bigint),cast(split(host_dur[0],'_')[2] as bigint)) ,0)) / sum(if(if(split(host_dur[0],'_')[0]='subscribe',cast(split(host_dur[1],'_')[2] as bigint),cast(split(host_dur[0],'_')[2] as bigint))  between 0 and 30000,1,0))) as decimal(16,2)) client_connect_signaling_duration,
        cast((sum(if(if(split(host_dur[0],'_')[0]='subscribe',cast(split(host_dur[0],'_')[1] as bigint),cast(split(host_dur[1],'_')[1] as bigint)) between 0 and 30000, if(split(host_dur[0],'_')[0]='subscribe',cast(split(host_dur[0],'_')[1] as bigint),cast(split(host_dur[1],'_')[1] as bigint)) ,0)) / sum(if(if(split(host_dur[0],'_')[0]='subscribe',cast(split(host_dur[0],'_')[1] as bigint),cast(split(host_dur[1],'_')[1] as bigint))  between 0 and 30000,1,0))) as decimal(16,2))  client_publish_signaling_duration,
        -1 client_ice_success_duration,
        cast(sum(if(sufst1 between 0 and 30000, sufst1 ,0)) / sum(if(sufst1  between 0 and 30000,1,0)) as decimal(16,2)) client_fst1 ,
       cast(sum(if(sufst2 between 0 and 30000, sufst2 ,0)) / sum(if(sufst2  between 0 and 30000,1,0)) as decimal(16,2))  client_fst2 ,
        cast(sum(if(sufst3 between 0 and 30000, sufst3 ,0)) / sum(if(sufst3  between 0 and 30000,1,0)) as decimal(16,2)) client_fst3 ,
        cast(sum(if(sufst4 between 0 and 30000, sufst4 ,0)) / sum(if(sufst4  between 0 and 30000,1,0)) as decimal(16,2)) client_fst4 ,
        -1 client_fst5,
        cast(sum(if(tar_rate_mean between 0 and 1000*1000, tar_rate_mean ,0)) / sum(if(tar_rate_mean between 0 and 1000*1000,1,0)) as decimal(16,2)) tar_rate_mean,
        percentile_approx(cast(tar_rate_mean as decimal(16,2)),
                                      array( 0.3, 0.5, 0.7, 0.9)) tar_rate_mean_pct,
        cast(sum(if(act_rate_mean between 0 and 1000*1000, act_rate_mean ,0)) / sum(if(act_rate_mean between 0 and 1000*1000,1,0)) as decimal(16,2)) act_rate_mean,
        percentile_approx(cast(act_rate_mean as decimal(16,2)),
                                      array( 0.3, 0.5, 0.7, 0.9)) act_rate_mean_pct,
        cast(sum(if(send_rate_mean between 0 and 1000*1000, send_rate_mean ,0)) / sum(if(send_rate_mean between 0 and 1000*1000,1,0)) as decimal(16,2)) send_rate_mean,
        percentile_approx(cast(send_rate_mean as decimal(16,2)),
                                      array( 0.3, 0.5, 0.7, 0.9)) send_rate_mean_pct,
        cast(sum(if(rcvd_rate_mean between 0 and 1000*1000, rcvd_rate_mean ,0)) / sum(if(rcvd_rate_mean between 0 and 1000*1000,1,0)) as decimal(16,2)) rcvd_rate_mean,
        percentile_approx(cast(rcvd_rate_mean as decimal(16,2)),
                                      array( 0.3, 0.5, 0.7, 0.9)) rcvd_rate_mean_pct,
        -1 rtx_rate_mean,-1 fec_rate_mean,-1 rtx_recover_rate,-1 fec_recover_rate,
        cast(avg(cap_fr_mean) as decimal(16,2)) cap_fr_mean,
        percentile_approx(cast(cap_fr_mean as decimal(16,2)) ,
                                      array( 0.3, 0.5, 0.7, 0.9)) cap_fr_mean_pct,
        cast(avg(tar_fr_mean) as decimal(16,2)) tar_fr_mean,
        percentile_approx(cast(tar_fr_mean as decimal(16,2)) ,
                                      array( 0.3, 0.5, 0.7, 0.9)) tar_fr_mean_pct,
        cast(avg(send_fr_mean) as decimal(16,2)) send_fr_mean,
        percentile_approx(cast(send_fr_mean as decimal(16,2)),
                                      array( 0.3, 0.5, 0.7, 0.9)) send_fr_mean_pct,
        cast(avg(rcvd_fr_mean) as decimal(16,2)) rcvd_fr_mean,
        percentile_approx(cast(rcvd_fr_mean as decimal(16,2)),
                                      array( 0.3, 0.5, 0.7, 0.9)) rcvd_fr_mean_pct,
        cast(avg(dec_fr_mean) as decimal(16,2)) dec_fr_mean,
        percentile_approx(cast(dec_fr_mean as decimal(16,2)),
                                      array( 0.3, 0.5, 0.7, 0.9)) dec_fr_mean_pct,
        cast(avg(rend_fr_mean) as decimal(16,2)) send_render_fr_mean,
        percentile_approx(cast(rend_fr_mean as decimal(16,2)),
                                      array( 0.3, 0.5, 0.7, 0.9)) send_render_fr_mean_pct,-1 act_render_fr_mean,
        cast(avg(es_bwe_mean) as decimal(16,2)) es_bwe_mean,
        percentile_approx(cast(es_bwe_mean as decimal(16,2)),
                                      array( 0.3, 0.5, 0.7, 0.9)) es_bwe_mean_pct,
        cast(avg(bwe_mean) as decimal(16,2)) bwe_mean,
        percentile_approx(cast(bwe_mean as decimal(16,2)),
                                      array( 0.3, 0.5, 0.7, 0.9)) bwe_mean_pct,
        cast(sum(if(rtt_mean between 0 and 34000, rtt_mean ,0)) / sum(if(rtt_mean  between 0 and 34000,1,0))  as decimal(16,2)) rtt_mean,
        percentile_approx(cast(rtt_mean as decimal(16,2)),
                                      array( 0.3, 0.5, 0.7, 0.9)) rtt_mean_pct,
        cast(avg(loss_mean *100) as decimal(16,2)) loss_mean,
        percentile_approx(cast(loss_mean as decimal(16,2)),
                                      array( 0.3, 0.5, 0.7, 0.9)) loss_mean_pct,
        -1 e2e_delay,
        cast(avg(dec_stuck_time / (1000 * conn_time) * 100) as DECIMAL(16, 2)) stuck_ratio,-1 avg_render_stuck_ratio,-1 render_duration_pct_90,-1 render_duration_pct_99,-1 render_duration_pct_999,-1 loss_frame_rate,
        cast(sum(if(qp_mean between 0 and 63, qp_mean ,0)) / sum(if(qp_mean between 0 and 63,1,0)) as decimal(16,2)) qp_mean,
        percentile_approx(cast(qp_mean as decimal(16,2)) ,
                                      array( 0.3, 0.5, 0.7, 0.9)) qp_mean_pct,
        count(1) c1,
	        concat_ws(',',
	    cast(cast(avg(vcap0) as decimal(16,2)) as string),
            cast(cast(avg(vcap1) as decimal(16,2)) as string),
            cast(cast(avg(vcap2) as decimal(16,2)) as string),
            cast(cast(avg(vcap3) as decimal(16,2)) as string),
            cast(cast(avg(vcap4) as decimal(16,2)) as string),
            cast(cast(avg(vcap5) as decimal(16,2)) as string),
            cast(cast(avg(vcap6) as decimal(16,2)) as string),
            cast(cast(avg(vcap7) as decimal(16,2)) as string)) vcap_detail,
            concat_ws(',',
	    cast(cast(avg(venc0) as decimal(16,2)) as string),
            cast(cast(avg(venc1) as decimal(16,2)) as string),
            cast(cast(avg(venc2) as decimal(16,2)) as string),
            cast(cast(avg(venc3) as decimal(16,2)) as string),
            cast(cast(avg(venc4) as decimal(16,2)) as string),
            cast(cast(avg(venc5) as decimal(16,2)) as string),
            cast(cast(avg(venc6) as decimal(16,2)) as string),
            cast(cast(avg(venc7) as decimal(16,2)) as string)) venc_detail,
            concat_ws(',',
	    cast(cast(avg(vpac0) as decimal(16,2)) as string),
            cast(cast(avg(vpac1) as decimal(16,2)) as string),
            cast(cast(avg(vpac2) as decimal(16,2)) as string),
            cast(cast(avg(vpac3) as decimal(16,2)) as string),
            cast(cast(avg(vpac4) as decimal(16,2)) as string),
            cast(cast(avg(vpac5) as decimal(16,2)) as string),
            cast(cast(avg(vpac6) as decimal(16,2)) as string),
            cast(cast(avg(vpac7) as decimal(16,2)) as string)) vpac_detail,
            concat_ws(',',
	    cast(cast(avg(vrec0) as decimal(16,2)) as string),
            cast(cast(avg(vrec1) as decimal(16,2)) as string),
            cast(cast(avg(vrec2) as decimal(16,2)) as string),
            cast(cast(avg(vrec3) as decimal(16,2)) as string),
            cast(cast(avg(vrec4) as decimal(16,2)) as string),
            cast(cast(avg(vrec5) as decimal(16,2)) as string),
            cast(cast(avg(vrec6) as decimal(16,2)) as string),
            cast(cast(avg(vrec7) as decimal(16,2)) as string)) vrec_detail,
            concat_ws(',',
	    cast(cast(avg(vjit0) as decimal(16,2)) as string),
            cast(cast(avg(vjit1) as decimal(16,2)) as string),
            cast(cast(avg(vjit2) as decimal(16,2)) as string),
            cast(cast(avg(vjit3) as decimal(16,2)) as string),
            cast(cast(avg(vjit4) as decimal(16,2)) as string),
            cast(cast(avg(vjit5) as decimal(16,2)) as string),
            cast(cast(avg(vjit6) as decimal(16,2)) as string),
            cast(cast(avg(vjit7) as decimal(16,2)) as string)) vjit_detail,
            concat_ws(',',
	    cast(cast(avg(vdec0) as decimal(16,2)) as string),
            cast(cast(avg(vdec1) as decimal(16,2)) as string),
            cast(cast(avg(vdec2) as decimal(16,2)) as string),
            cast(cast(avg(vdec3) as decimal(16,2)) as string),
            cast(cast(avg(vdec4) as decimal(16,2)) as string),
            cast(cast(avg(vdec5) as decimal(16,2)) as string),
            cast(cast(avg(vdec6) as decimal(16,2)) as string),
            cast(cast(avg(vdec7) as decimal(16,2)) as string)) vdec_detail,
            concat_ws(',',
	    cast(cast(avg(vstk0) as decimal(16,2)) as string),
            cast(cast(avg(vstk1) as decimal(16,2)) as string),
            cast(cast(avg(vstk2) as decimal(16,2)) as string),
            cast(cast(avg(vstk3) as decimal(16,2)) as string),
            cast(cast(avg(vstk4) as decimal(16,2)) as string),
            cast(cast(avg(vstk5) as decimal(16,2)) as string),
            cast(cast(avg(vstk6) as decimal(16,2)) as string),
            cast(cast(avg(vstk7) as decimal(16,2)) as string)) vstk_detail,
            concat_ws(',',
	    cast(cast(avg(vrtc0) as decimal(16,2)) as string),
            cast(cast(avg(vrtc1) as decimal(16,2)) as string),
            cast(cast(avg(vrtc2) as decimal(16,2)) as string),
            cast(cast(avg(vrtc3) as decimal(16,2)) as string),
            cast(cast(avg(vrtc4) as decimal(16,2)) as string),
            cast(cast(avg(vrtc5) as decimal(16,2)) as string),
            cast(cast(avg(vrtc6) as decimal(16,2)) as string),
            cast(cast(avg(vrtc7) as decimal(16,2)) as string)) vrtc_detail,
            concat_ws(',',
	    cast(cast(avg(vqp0) as decimal(16,2)) as string),
            cast(cast(avg(vqp1) as decimal(16,2)) as string),
            cast(cast(avg(vqp2) as decimal(16,2)) as string),
            cast(cast(avg(vqp3) as decimal(16,2)) as string),
            cast(cast(avg(vqp4) as decimal(16,2)) as string),
            cast(cast(avg(vqp5) as decimal(16,2)) as string),
            cast(cast(avg(vqp6) as decimal(16,2)) as string),
            cast(cast(avg(vqp7) as decimal(16,2)) as string)) vqp_detail,
	    concat_ws(',',cast(cast(avg(bur_max) as decimal(16,2)) as string),
            cast(cast(avg(bur_min)   as decimal(16,2))as string),
            cast(cast(avg(bur_mean)  as decimal(16,2)) as  string)) bur_detail,
            concat_ws(',',cast(cast(avg(next_delay_max) as decimal(16,2))as string) ,
            cast(cast(avg(next_delay_min  )  as decimal(16,2))as string),
            cast(cast(avg(next_delay_mean )  as decimal(16,2))as string)) next_delay_detail,
            concat_ws(',',cast(cast(avg(frm_recv_rate_max) as decimal(16,2)) as string),
            cast(cast(avg(frm_recv_rate_min )   as decimal(16,2))as string),
            cast(cast(avg(frm_recv_rate_mean )  as decimal(16,2))  as string)) frm_recv_rate_detail,
            concat_ws(',',cast(cast(avg(frm_drain_rate_max ) as decimal(16,2)) as string),
            cast(cast(avg(frm_drain_rate_min )   as decimal(16,2))as string),
            cast(cast(avg(frm_drain_rate_mean )  as decimal(16,2)) as string)) frm_drain_rate_detail
    from
    (
        select *
        from
        (
            select
                conn_id,platform client_platform ,version client_version,
                case
                     when network = '电信' then '电信'
                     when network = '移动' then '移动'
                     when network = '联通' then '联通'
                     when network like '%教育网%' then '教育'
                     else 'other' end client_network,
                if(conn_type is null,-1,conn_type) conn_type,
                concat_ws('_',if(codec_name is null ,'other',codec_name) ,if(\`dec\` is null ,'other',\`dec\` )) client_codec_name,
                cast(first_frame_time as bigint) first_frame_time,
                cast(fst1 as bigint)         sufst1,
                cast(fst2 as bigint)         sufst2,
                cast(fst3 as bigint)         sufst3,
                cast(fst4 as bigint)         sufst4,
                cast(rcvd_rate_mean as bigint)         rcvd_rate_mean,
                cast(rcvd_fr_mean as bigint)         rcvd_fr_mean,
                cast(dec_fr_mean as bigint)         dec_fr_mean,
                cast(rend_fr_mean as bigint)         rend_fr_mean,
                cast(dec_stuck_time as bigint)         dec_stuck_time,
                cast(conn_time as bigint)         conn_time,
		        cast(split(vcap,'\\\\|')[0] as bigint) vcap0,
        cast(split(vcap,'\\\\|')[1] as bigint) vcap1,
        cast(split(vcap,'\\\\|')[2] as bigint) vcap2,
        cast(split(vcap,'\\\\|')[3] as bigint) vcap3,
        cast(split(vcap,'\\\\|')[4] as bigint) vcap4,
        cast(split(vcap,'\\\\|')[5] as bigint) vcap5,
        cast(split(vcap,'\\\\|')[6] as bigint) vcap6,
        cast(split(vcap,'\\\\|')[7] as bigint) vcap7,
        cast(split(venc,'\\\\|')[0] as bigint) venc0,
        cast(split(venc,'\\\\|')[1] as bigint) venc1,
        cast(split(venc,'\\\\|')[2] as bigint) venc2,
        cast(split(venc,'\\\\|')[3] as bigint) venc3,
        cast(split(venc,'\\\\|')[4] as bigint) venc4,
        cast(split(venc,'\\\\|')[5] as bigint) venc5,
        cast(split(venc,'\\\\|')[6] as bigint) venc6,
        cast(split(venc,'\\\\|')[7] as bigint) venc7,
        cast(split(vpac,'\\\\|')[0] as bigint) vpac0,
        cast(split(vpac,'\\\\|')[1] as bigint) vpac1,
        cast(split(vpac,'\\\\|')[2] as bigint) vpac2,
        cast(split(vpac,'\\\\|')[3] as bigint) vpac3,
        cast(split(vpac,'\\\\|')[4] as bigint) vpac4,
        cast(split(vpac,'\\\\|')[5] as bigint) vpac5,
        cast(split(vpac,'\\\\|')[6] as bigint) vpac6,
        cast(split(vpac,'\\\\|')[7] as bigint) vpac7,
        cast(split(vrec,'\\\\|')[0] as bigint) vrec0,
        cast(split(vrec,'\\\\|')[1] as bigint) vrec1,
        cast(split(vrec,'\\\\|')[2] as bigint) vrec2,
        cast(split(vrec,'\\\\|')[3] as bigint) vrec3,
        cast(split(vrec,'\\\\|')[4] as bigint) vrec4,
        cast(split(vrec,'\\\\|')[5] as bigint) vrec5,
        cast(split(vrec,'\\\\|')[6] as bigint) vrec6,
        cast(split(vrec,'\\\\|')[7] as bigint) vrec7,
        cast(split(vjit,'\\\\|')[0] as bigint) vjit0,
        cast(split(vjit,'\\\\|')[1] as bigint) vjit1,
        cast(split(vjit,'\\\\|')[2] as bigint) vjit2,
        cast(split(vjit,'\\\\|')[3] as bigint) vjit3,
        cast(split(vjit,'\\\\|')[4] as bigint) vjit4,
        cast(split(vjit,'\\\\|')[5] as bigint) vjit5,
        cast(split(vjit,'\\\\|')[6] as bigint) vjit6,
        cast(split(vjit,'\\\\|')[7] as bigint) vjit7,
        cast(split(vdec,'\\\\|')[0] as bigint) vdec0,
        cast(split(vdec,'\\\\|')[1] as bigint) vdec1,
        cast(split(vdec,'\\\\|')[2] as bigint) vdec2,
        cast(split(vdec,'\\\\|')[3] as bigint) vdec3,
        cast(split(vdec,'\\\\|')[4] as bigint) vdec4,
        cast(split(vdec,'\\\\|')[5] as bigint) vdec5,
        cast(split(vdec,'\\\\|')[6] as bigint) vdec6,
        cast(split(vdec,'\\\\|')[7] as bigint) vdec7,
        cast(split(vstk,'\\\\|')[0] as bigint) vstk0,
        cast(split(vstk,'\\\\|')[1] as bigint) vstk1,
        cast(split(vstk,'\\\\|')[2] as bigint) vstk2,
        cast(split(vstk,'\\\\|')[3] as bigint) vstk3,
        cast(split(vstk,'\\\\|')[4] as bigint) vstk4,
        cast(split(vstk,'\\\\|')[5] as bigint) vstk5,
        cast(split(vstk,'\\\\|')[6] as bigint) vstk6,
        cast(split(vstk,'\\\\|')[7] as bigint) vstk7,
        cast(split(vrtc,'\\\\|')[0] as bigint) vrtc0,
        cast(split(vrtc,'\\\\|')[1] as bigint) vrtc1,
        cast(split(vrtc,'\\\\|')[2] as bigint) vrtc2,
        cast(split(vrtc,'\\\\|')[3] as bigint) vrtc3,
        cast(split(vrtc,'\\\\|')[4] as bigint) vrtc4,
        cast(split(vrtc,'\\\\|')[5] as bigint) vrtc5,
        cast(split(vrtc,'\\\\|')[6] as bigint) vrtc6,
        cast(split(vrtc,'\\\\|')[7] as bigint) vrtc7,
        cast(split(vqp,'\\\\|')[0] as bigint) vqp0,
        cast(split(vqp,'\\\\|')[1] as bigint) vqp1,
        cast(split(vqp,'\\\\|')[2] as bigint) vqp2,
        cast(split(vqp,'\\\\|')[3] as bigint) vqp3,
        cast(split(vqp,'\\\\|')[4] as bigint) vqp4,
        cast(split(vqp,'\\\\|')[5] as bigint) vqp5,
        cast(split(vqp,'\\\\|')[6] as bigint) vqp6,
        cast(split(vqp,'\\\\|')[7] as bigint) vqp7
            from ${APP}.dwd_sdk_su
            where dt='$do_date'
            and success_num=over_num
            and platform is not null
            and version is not null
        )t1
        inner join
        (
            select
                conn_id connid,platform host_platform,version host_version,
                case
                     when network = '电信' then '电信'
                     when network = '移动' then '移动'
                     when network = '联通' then '联通'
                     when network like '%教育网%' then '教育'
                     else 'other' end host_network,
                concat_ws('_',if(codec_name is null ,'other',codec_name),if(enc is null ,'other',enc )) host_codec_name,
                cast(fst1 as bigint) dufst1,
                cast(fst2 as bigint) dufst2,
                cast(fst3 as bigint) dufst3,
                cast(fst4 as bigint) dufst4,
                cast(fst5 as bigint) dufst5,
                cast(tar_rate_mean as bigint) tar_rate_mean,
                cast(act_rate_mean as bigint) act_rate_mean,
                cast(send_rate_mean as bigint) send_rate_mean,
                cast(cap_fr_mean as bigint) cap_fr_mean,
                cast(tar_fr_mean as bigint) tar_fr_mean,
                cast(send_fr_mean as bigint) send_fr_mean,
                cast(es_bwe_mean as bigint) es_bwe_mean,
                cast(bwe_mean as bigint) bwe_mean,
                cast(rtt_mean as bigint) rtt_mean,
                cast(loss_mean as double) loss_mean,
                cast(qp_mean as bigint) qp_mean,
                if(local_proto is null,'forward',local_proto) local_proto,
                tracks_size track_count,
		cast(bur_max as decimal(16,2)) bur_max,
		cast(bur_min as decimal(16,2)) bur_min,
		cast(bur_mean as decimal(16,2)) bur_mean,
		cast(next_delay_max as decimal(16,2)) next_delay_max,
		cast(next_delay_min as decimal(16,2)) next_delay_min,
		cast(next_delay_mean as decimal(16,2)) next_delay_mean,
		cast(frm_recv_rate_max as decimal(16,2)) frm_recv_rate_max,
		cast(frm_recv_rate_min as decimal(16,2)) frm_recv_rate_min,
		cast(frm_recv_rate_mean as decimal(16,2)) frm_recv_rate_mean,
		cast(frm_drain_rate_max as decimal(16,2)) frm_drain_rate_max,
		cast(frm_drain_rate_min as decimal(16,2)) frm_drain_rate_min,
		cast(frm_drain_rate_mean as decimal(16,2)) frm_drain_rate_mean
            from ${APP}.dwd_sdk_du
            where dt='$do_date'
            and success_num=over_num
            and platform is not null
            and version is not null
        )t2
        on t1.conn_id=t2.connid
        where first_frame_time between -1 and 25000
         and    dufst1 between -1 and 30000
         and    dufst2 between -1 and 30000
         and    dufst3 between -1 and 30000
         and    dufst4 between -1 and 30000
         and    dufst5 between -1 and 30000
         and    sufst1 between -1 and 30000
         and    sufst2 between -1 and 30000
         and    sufst3 between -1 and 30000
         and    sufst4 between -1 and 30000
         and    tar_rate_mean between 0 and 1000*1000
         and    tar_rate_mean between 0 and 1000*1000
         and    send_rate_mean between 0 and 1000*1000
         and    rcvd_rate_mean between 0 and 1000*1000
         and    qp_mean between 0 and 63
         and    rtt_mean between 0 and 34000
	 and (vcap7 is null or vcap7 < 10000)
	and (venc7 is null or venc7 < 10000)
              and (vpac7 is null or vpac7 < 10000)
              and (vrec7 is null or vrec7 < 10000)
              and (vjit7 is null or vjit7 < 10000)
              and (vdec7 is null or vdec7 < 10000)
              and (vrtc7 is null or vrtc7 < 10000)
              and (vstk7 is null or vstk7 < 20000)
              and (vqp7  is null or vqp7 between 0 and 64)
    )t1
    left join
    (
        select
            connid,
            case when labtype='daas' then if(scountry='中国' and sprovince is not null and sprovince !='',sprovince,'other')
                 when labtype='excalibur' then '-1'
            else '-1' end sprovince,
            case when labtype='daas' then if(dcountry='中国' and dprovince is not null and dprovince !='',dprovince,'other')
                 when labtype='excalibur' then '-1'
            else '-1' end dprovince,
            if(labtype='excalibur','todesk_c',labtype) labtype,resultmsg,if(id>100000000,1,0) trails_user
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        and labtype in ('daas','excalibur','cloudapp')
    )t3
    on t1.conn_id=t3.connid
    left join
    (
        select
            conn_id,collect_list(concat_ws('_',action,duration)) host_dur
        from
        (
            select
                conn_id,action,cast(duration as string) duration,ts,min(ts) over(partition by conn_id,action) min_ts
            from ${APP}.dwd_client_licode_detail
            where dt='$do_date' and clientype='host' and action in ('enter_room','publish')
        )t1
        where ts = min_ts
        group by conn_id
    )t4
    on t1.conn_id=t4.conn_id
    left join
    (
        select
            conn_id,collect_list(concat_ws('_',action,duration)) client_dur
        from
        (
            select
                conn_id,action,cast(duration as string) duration,ts,min(ts) over(partition by conn_id,action) min_ts
            from ${APP}.dwd_client_licode_detail
            where dt='$do_date' and clientype='client' and action in ('enter_room','subscribe')
        )t1
        where ts = min_ts
        group by conn_id
    )t5
    on t1.conn_id=t5.conn_id
    group by labtype,client_platform,host_platform,client_version,host_version,client_network,host_network,sprovince,dprovince,conn_type,client_codec_name,host_codec_name,track_count,resultmsg,trails_user,local_proto
)t1;"

ads_platform_file_detail_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_platform_file_detail_total
select
    '$do_date',labtype,
    client_platform,host_platform,client_version,host_version,'-1','-1','-1','-1',conn_type,channel_type,scvip,
    trans_type,is_vc,
    cast (sum(if(fail_reason=0, 1, 0)) / count(1) as decimal(16,4)),
    cast (avg(if(eff_speed != 0, eff_speed, null)) as decimal(16,4) ) ,
    cast (sum(if(inv_fragment!=0,1,0)) / count(1) as decimal(16,4)),
    cast (sum(if(fail_reason=1, 1, 0)) / count(1) as decimal(16,4)),
    cast (sum(if(fail_reason=2, 1, 0)) / count(1) as decimal(16,4)),
    cast (sum(if(fail_reason=3, 1, 0)) / count(1) as decimal(16,4)),
    cast (sum(if(fail_reason=4, 1, 0)) / count(1) as decimal(16,4)),
    cast (sum(if(fail_reason=5, 1, 0)) / count(1) as decimal(16,4)),
    cast (sum(if(fail_reason=6, 1, 0)) / count(1) as decimal(16,4)),
    count(1) as total_cnt
from
(
    select
        *,if (trans_time >= 2000 AND file_size >= 10*1024*1024, file_size/trans_time, 0) as eff_speed,
        if(inv_fragment1 is not null,inv_fragment1,0)+if(inv_fragment2 is not null,inv_fragment2,0) as inv_fragment,
        case when trans_channel = 5
        then 'ru'
        when trans_channel = 6
        then 'rp'
        else 'forward' end as conn_type,
        case when is_vc = true and trans_channel >= 5
        then 'virtual channel'
        when is_vc = false and trans_channel >= 5
        then 'data channel'
        else 'forward channel' end as channel_type
    from
    (
        select
            scvip,labtype,
            client_version,
            host_version,
            client_platform,
            host_platform,if(t3.conn_id is null, '0', '1') as is_vc,
            fail_reason,
            trans_type,
            eff_fragment,
            inv_fragment1,
            inv_fragment2,
            file_size,
            trans_time,
            new_transfer,
            trans_channel
        from
        (
            select conn_id,
                   cast(get_json_object(errno,'$.fail_reason') as decimal(16,2)) fail_reason,
                   cast(get_json_object(errno,'$.trans_type') as string) trans_type,
                   cast(get_json_object(errno,'$.eff_fragment') as decimal(16,2)) eff_fragment,
                   cast(get_json_object(errno,'$.inv_fragment') as decimal(16,2)) inv_fragment1,
                   cast(get_json_object(errno,'$.invalid_fragment') as decimal(16,2)) inv_fragment2,
                   cast(get_json_object(errno,'$.file_size') as decimal(16,2)) file_size,
                   cast(get_json_object(errno,'$.trans_time') as decimal(16,2)) trans_time,
                   cast(get_json_object(errno,'$.new_transfer') as decimal(16,2)) new_transfer,
                   cast(get_json_object(errno,'$.trans_channel') as decimal(16,2)) trans_channel
            from ${APP}.dwd_client_tevent
            where dt = '$do_date' AND clientype='file' AND eventtype='statistics' AND nettype='client'
        )t1
        inner join
        (
            select connid, sversion as client_version, dversion as host_version,
                    splatform as client_platform, dplatform as host_platform,if(labtype='excalibur','todesk_c',labtype) labtype,
                    case when scvip in ('0','1') then '免费版'
                         when scvip ='2' then '专业版'
                         when scvip ='3' then '团队版'
                         when scvip ='4' then '专业长期'
                         when scvip ='5' then '团队长期'
                         when scvip ='6' then '高性能预埋'
                         when scvip ='10' then '游戏版'
                    else '其他' end scvip
            from ${APP}.dwd_newfile_connect
            where dt = '$do_date'
            and labtype in ('daas','excalibur')
        )t2
        on t1.conn_id=t2.connid
        left join
        (
            select version, conn_id
            from ${APP}.dwd_vc_du
            where dt = '$do_date'
        )t3
        on t2.connid=t3.conn_id
    )t1
)t2
group by labtype,client_platform,host_platform,client_version,host_version,conn_type,trans_type,channel_type,scvip,is_vc;"

ads_platform_vc_detail_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_platform_vc_detail_total
select
    '$do_date','-1',
    client_platform,
    host_platform,
    client_version,
    host_version,'-1','-1','-1','-1','-1',label,
    direction,'-1',
    cast (sum(recv_unkown_packet_cnt + recv_parse_fail_cnt + retrans_invalid_cnt) / sum(recv_parse_success_cnt + recv_parse_fail_cnt)  as decimal(16,4)),
    cast (sum(recv_data_packet_cnt) / sum(send_data_packet_cnt) as decimal(16,4)),
    -1,-1,-1,'-1',-1,'-1',
       -1,'-1',-1,'-1',-1,'-1',-1,'-1',-1,'-1',
    count(1)
from
(
    select
    host_label as label,
    client_version,
    host_version,
    client_platform,
    host_platform,
    'download' as direction,
    host_send_control_packet_cnt as send_control_packet_cnt,
    host_send_data_packet_cnt as send_data_packet_cnt,
    host_send_data_packet_retrans_cnt as send_data_packet_retrans_cnt,
    host_send_unkown_packet_cnt as send_unkown_packet_cnt,
    client_recv_parse_success_cnt as recv_parse_success_cnt,
    client_recv_parse_fail_cnt as recv_parse_fail_cnt,
    client_recv_control_packet_cnt as recv_control_packet_cnt,
    client_recv_data_packet_cnt as recv_data_packet_cnt,
    client_recv_data_packet_retrans_cnt as recv_data_packet_retrans_cnt,
    client_recv_unkown_packet_cnt as recv_unkown_packet_cnt,
    host_loss_cnt as loss_cnt,
    host_send_cnt as send_cnt,
    host_retrans_invalid_cnt as retrans_invalid_cnt
    from(
        select
            version as host_version,
            conn_id,
            platform as host_platform,
            label as host_label,
            out_cp_cnt as host_send_control_packet_cnt,
            out_dp_cnt as host_send_data_packet_cnt,
            out_dp_retrans_cnt as host_send_data_packet_retrans_cnt,
            out_dp_unkown_cnt as host_send_unkown_packet_cnt,
            loss_cnt as host_loss_cnt,
            send_cnt as host_send_cnt,
            retrans_invalid_cnt as host_retrans_invalid_cnt
        from ${APP}.dwd_vc_du
        where dt = '$do_date'
    ) a inner join (
        select
            version as client_version,
            conn_id,
            platform as client_platform,
            label as client_label,
            parse_success_cnt as client_recv_parse_success_cnt,
            parse_fail_cnt as client_recv_parse_fail_cnt,
            in_cp_cnt as client_recv_control_packet_cnt,
            in_dp_cnt as client_recv_data_packet_cnt,
            in_dp_retrans_cnt as client_recv_data_packet_retrans_cnt,
            in_dp_unkown_cnt as client_recv_unkown_packet_cnt
            from ${APP}.dwd_vc_su
        where dt = '$do_date'
    ) b on  a.conn_id = b.conn_id and a.host_label = b.client_label
)t1
group by label, host_version, client_version, host_platform, client_platform, direction
union all
select
    '$do_date','-1',
    client_platform,
    host_platform,
    client_version,
    host_version,'-1','-1','-1','-1','-1',label,
    direction,'-1',
    cast (sum(recv_unkown_packet_cnt + recv_parse_fail_cnt + retrans_invalid_cnt) / sum(recv_parse_success_cnt + recv_parse_fail_cnt)  as decimal(16,4)),
    cast (sum(recv_data_packet_cnt) / sum(send_data_packet_cnt) as decimal(16,4)),
    -1,-1,-1,'-1',-1,'-1',
       -1,'-1',-1,'-1',-1,'-1',-1,'-1',-1,'-1',
    count(1)
from
(
    select
    host_label as label,
    host_version,
    client_version,
    host_platform,
    client_platform,
    'upload' as direction,
    client_send_control_packet_cnt as send_control_packet_cnt,
    client_send_data_packet_cnt as send_data_packet_cnt,
    client_send_data_packet_retrans_cnt as send_data_packet_retrans_cnt,
    client_send_unkown_packet_cnt as send_unkown_packet_cnt,
    host_recv_parse_success_cnt as recv_parse_success_cnt,
    host_recv_parse_fail_cnt as recv_parse_fail_cnt,
    host_recv_control_packet_cnt as recv_control_packet_cnt,
    host_recv_data_packet_cnt as recv_data_packet_cnt,
    host_recv_data_packet_retrans_cnt as recv_data_packet_retrans_cnt,
    host_recv_unkown_packet_cnt as recv_unkown_packet_cnt,
    client_loss_cnt as loss_cnt,
    client_send_cnt as send_cnt,
    client_retrans_invalid_cnt as retrans_invalid_cnt
    from(
    select
        conn_id,
        version as host_version,
        platform as host_platform,
        label as host_label,
        parse_success_cnt as host_recv_parse_success_cnt,
        parse_fail_cnt as host_recv_parse_fail_cnt,
        in_cp_cnt as host_recv_control_packet_cnt,
        in_dp_cnt as host_recv_data_packet_cnt,
        in_dp_retrans_cnt as host_recv_data_packet_retrans_cnt,
        in_dp_unkown_cnt as host_recv_unkown_packet_cnt
    from ${APP}.dwd_vc_du
    where dt = '$do_date'
    ) a inner join (
    select
    conn_id,
    version as client_version,
    platform as client_platform,
    label as client_label,
    out_cp_cnt as client_send_control_packet_cnt,
    out_dp_cnt as client_send_data_packet_cnt,
    out_dp_retrans_cnt as client_send_data_packet_retrans_cnt,
    out_dp_unkown_cnt as client_send_unkown_packet_cnt,
    loss_cnt as client_loss_cnt,
    send_cnt as client_send_cnt,
    retrans_invalid_cnt as client_retrans_invalid_cnt
    from ${APP}.dwd_vc_su
    where dt = '$do_date'
    ) b on a.conn_id = b.conn_id and a.host_label = b.client_label
)t1
group by label, host_version, client_version, host_platform, client_platform, direction;"

ads_daas_user_total="
insert overwrite table ${APP}.ads_daas_user_total
select
    '$do_date',count(1),sum(c1)
from
(
    select
        \`user\`,count(1) c1
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and labtype='daas'
    group by \`user\`
)t1;"

ads_daas_forward_failed_total="
insert overwrite table ${APP}.ads_daas_forward_failed_total
select
    '$do_date',if(reason is null,'other',reason),count(1)
from
(
    select
        t1.connid newconnect_connid,t2.connid handshake_connid,\`user\`
    from
    (
        select
            connid,ts connect_ts,suid,duid,\`user\`
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        and labtype='daas'
        and relay_server_name is not null
        and relay_server_name !=''
        and resultmsg!='dbc'
    )t1
    left join
    (
        select
          connid
        from ${APP}.dwd_handshake_sd
        where dt='$do_date'
        and labtype='daas'
        and type='presenter'
        group by connid
    )t2
    on t1.connid=t2.connid
)t1
left join
(
    select
        connid reconnect_connid,'reconnect' reason
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and labtype='daas'
    and relay_server_name is not null
    and relay_server_name !=''
    and resultmsg!='dbc'
    and reconnect='1'
    group by connid
    union
    select
            connid ping_timeout_connid,'ping_timeout'
    from
    (
        select
            t1.*,t1.connect_ts-t3.disconnected_ts,lag(connect_ts,1,disconnected_ts) over(partition by t1.suid,t1.duid,disconnected_connid order by connect_ts) ping_ts
        from
        (
            select
                connid,ts connect_ts,suid,duid
            from ${APP}.dwd_new_connect
            where dt='$do_date'
            and labtype='daas'
            and relay_server_name is not null
            and relay_server_name !=''
            and resultmsg!='dbc'
            and reconnect='0'
        )t1
        left join
        (
            select
              connid handshake_sd_connid
            from ${APP}.dwd_handshake_sd
            where dt='$do_date'
            and labtype='daas'
            and type='presenter'
            group by connid
        )t2
        on t1.connid=t2.handshake_sd_connid
        left join
        (
            select
                connid disconnected_connid,suid,duid,disconnected_ts
            from
            (
                select
                    connid,suid,duid
                from ${APP}.dwd_new_connect
                where dt='$do_date'
                and labtype='daas'
                and relay_server_name is not null
                and relay_server_name !=''
                and resultmsg!='dbc'
                and reconnect='0'
            )t1
            inner join
            (
                select
                  conn_id,logtime disconnected_ts
                from ${APP}.dwd_backend_licode_socket_disconnected
                where dt='$do_date'
                and type='presenter'
                and reason='ping timeout'
            )t2
            on t1.connid=t2.conn_id
        )t3
        on t1.duid=t3.duid and t1.suid=t3.suid
        where t2.handshake_sd_connid is null
        and t1.connect_ts-t3.disconnected_ts>0
    )t1
    where connect_ts-ping_ts<2*60*1000
    group by connid
    union
    select
        t1.connid disconnected_connid,'licode_timeout'
    from(
        select
            connid,ts connect_ts,suid,duid
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        and labtype='daas'
        and relay_server_name is not null
        and relay_server_name !=''
        and resultmsg!='dbc'
        and reconnect='0'
    )t1
    left join
    (
        select
            *
        from
        (
            select
                connid,ts success_ts,suid,duid
            from ${APP}.dwd_session_success
            where dt='$do_date' and labtype='daas'
        )t1
        inner join
        (
            select
              conn_id,logtime disconnected_ts
            from ${APP}.dwd_backend_licode_socket_disconnected
            where dt='$do_date'
            and type='presenter'
        )t2
        on t1.connid=t2.conn_id
    )t2
    on t1.suid=t2.suid and t1.duid=t2.duid
    where connect_ts>success_ts and connect_ts<disconnected_ts
    group by t1.connid
)t2
on t1.newconnect_connid=reconnect_connid
where handshake_connid is null
group by reason;"

ads_daas_client_session_day="
insert overwrite table ${APP}.ads_daas_client_session_day
select
    '$do_date',
    sclient,
    flag,
    sum(if(handshake_success_connid is not null,1,0)) c1,
    sum(if(authentication_success_connid is not null,1,0)) c1,
    count(1) c2,
    cast(sum(if(handshake_success_connid is not null,1,0)) /count(t1.connid) * 100 as decimal(16,2)),
    cast(sum(if(authentication_success_connid is not null,1,0)) /count(t1.connid) * 100 as decimal(16,2)),
    sum(if(t4.conn_id is not null and t2.authentication_success_connid is not null ,1,0)),
    sum(if(t4.conn_id is not null and t2.authentication_success_connid is not null ,link_time,0))
from
(
    select
        connid,
        case when sclient='1' then 'windows'
             when sclient='2' then 'mac'
             when sclient='3' then 'ios'
             when sclient='4' then 'android'
             when sclient='5' then 'linux'
             when sclient='6' then 'onther'
             when sclient='8' then 'web'
	     when sclient='9' then 'androidpad'
        else 'other' end sclient,if(cast(id as bigint)>100000000,1,0) flag
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and labtype='daas'
    and resultmsg!='dbc'
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
left join
(
    select
        t1.conn_id
    from
     (
        select
            conn_id
        from ${APP}.dwd_backend_licode_newconnect
        where dt='$do_date'
        and type='viewer'
        group by conn_id
     )t1
    inner join
     (
        select
            conn_id
        from ${APP}.dwd_backend_licode_newconnect
        where dt='$do_date'
        and type='presenter'
        group by conn_id
     )t2
    on t1.conn_id=t2.conn_id
)t4
on t1.connid=t4.conn_id
left join
(
    select
        connid,
        link_time
    from ${APP}.dwd_session_success
    where dt='$do_date'
    group by connid,link_time
)t5
on t1.connid=t5.connid
group by sclient,flag;"

ads_ec_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_ec_total
select
   '$do_date',clientype,platform,sum(if(turndata_connected='1',1,0)),count(1),cast(sum(if(turndata_connected='1',1,0))/count(1) *100 as decimal(16,2))
from ${APP}.dwd_licode_sig
where dt='$do_date'
group by clientype,platform;"

ads_todesk_forward_failed_total="
WITH newconnect AS (
    SELECT
        connid, dt, reconnect
    FROM ${APP}.dwd_new_connect
    WHERE dt ='$do_date'
    AND sclient = 'PC'
    AND dclient = 'PC'
    and labtype='excalibur'
),
handshake_success AS (
    SELECT
        connid, dt
    FROM ${APP}.dwd_handshake_success
    WHERE dt ='$do_date'
    GROUP BY connid, dt
),
su_handshake AS (
    SELECT
        connid, dt, COUNT(1) c1
    FROM ${APP}.dwd_handshake_sd
    WHERE dt ='$do_date'
    AND type = 'controller'
    GROUP BY connid, dt
),
du_handshake AS (
    SELECT
        connid, dt, CAST(MIN(ts) / 1000 AS BIGINT) du_ts 
    FROM ${APP}.dwd_handshake_sd
    WHERE dt ='$do_date'
    AND type = 'presenter'
    GROUP BY dt, connid
),
su_max_ts AS (
    SELECT 
        tb.dt dt, tb.connid connid, tc.ts ts, c1
    FROM
    (
        SELECT
            dt, connid, c1
        FROM su_handshake
        WHERE c1 IN (1, 2)
    ) tb
    LEFT JOIN
    (
        SELECT
            connid, CAST(MAX(ts) / 1000 AS BIGINT) ts, dt
        FROM ${APP}.dwd_handshake_sd
        WHERE dt ='$do_date'
        AND type = 'controller'
        GROUP BY dt, connid
    ) tc
    ON tb.connid = tc.connid AND tb.dt = tc.dt
),
su_forward AS (
    SELECT
        conn_id, dt
    FROM ${APP}.dwd_client_tevent
    WHERE dt ='$do_date'
    AND nettype = 'center'
    AND clientype = '0'
),
du_forward AS (
    SELECT
        conn_id, dt
    FROM ${APP}.dwd_client_tevent
    WHERE dt ='$do_date'
    AND nettype = 'center'
    AND clientype = '1'
),
su_forward_detail AS (
    SELECT  
        conn_id,
        action,
        get_json_object(errno, '$.errno') err,
        dt
    FROM ${APP}.dwd_client_tevent
    WHERE dt ='$do_date'
    AND action IN ('conful', 'conerr', 'consuc')
    AND eventtype = 'tran'
    AND clientype = '0'
),
du_forward_detail AS (
    SELECT
        conn_id,
        action,
        get_json_object(errno, '$.errno') err,
        dt
    FROM ${APP}.dwd_client_tevent
    WHERE dt ='$do_date'
    AND action IN ('conful', 'conerr', 'consuc', 'noOperation')
    AND eventtype = 'tran'
    AND clientype = '1'
),
total AS (
    SELECT
        dt,
        reconnect,
        type,
        COUNT(1) c1
    FROM
    (
        SELECT
            newconnect.dt dt, reconnect,
            CASE
                WHEN handshake_success.connid IS NOT NULL THEN 'handshake_success'
                WHEN handshake_success.connid IS NULL AND su_handshake.connid IS NULL AND su_forward.conn_id IS NULL THEN 'c4.1'
                WHEN handshake_success.connid IS NULL AND su_handshake.connid IS NULL AND su_forward.conn_id IS NOT NULL THEN (
                    CASE
                        WHEN su_forward_detail.action IS NULL THEN 'c4.2.3'
                        WHEN su_forward_detail.action = 'conerr' THEN 'c4.2.1'
                        WHEN su_forward_detail.action = 'conful' THEN 'c4.2.2'
                        WHEN su_forward_detail.action = 'consuc' THEN 'c4.2.4'
                    END
                )
                WHEN handshake_success.connid IS NULL AND su_handshake.connid IS NOT NULL AND du_handshake.connid IS NOT NULL AND du_ts > su_max_ts.ts THEN 'c3.1.1'
                WHEN handshake_success.connid IS NULL AND su_handshake.connid IS NOT NULL AND du_handshake.connid IS NOT NULL AND du_ts <= su_max_ts.ts THEN 'c3.1.2'
                WHEN handshake_success.connid IS NULL AND su_handshake.connid IS NOT NULL AND du_handshake.connid IS NULL AND du_forward.conn_id IS NULL THEN 'c3.2.1'
                WHEN handshake_success.connid IS NULL AND su_handshake.connid IS NOT NULL AND du_handshake.connid IS NULL AND du_forward.conn_id IS NOT NULL THEN  
                    (CASE
                        WHEN du_forward_detail.action IS NULL THEN 'c3.2.2.3'
                        WHEN du_forward_detail.action = 'conerr' THEN 'c3.2.2.1'
                        WHEN du_forward_detail.action = 'conful' THEN 'c3.2.2.2'
                        WHEN du_forward_detail.action = 'consuc' THEN 'c3.2.2.4'
                        WHEN du_forward_detail.action = 'noOperation' THEN 'c3.2.2.5'
                    END)
            END type
        FROM
            newconnect
            LEFT JOIN handshake_success ON newconnect.connid = handshake_success.connid AND newconnect.dt = handshake_success.dt
            LEFT JOIN su_handshake ON newconnect.connid = su_handshake.connid AND newconnect.dt = su_handshake.dt
            LEFT JOIN du_handshake ON newconnect.connid = du_handshake.connid AND newconnect.dt = du_handshake.dt
            LEFT JOIN su_forward ON newconnect.connid = su_forward.conn_id AND newconnect.dt = su_forward.dt
            LEFT JOIN du_forward ON newconnect.connid = du_forward.conn_id AND newconnect.dt = du_forward.dt
            LEFT JOIN su_max_ts ON newconnect.connid = su_max_ts.connid AND newconnect.dt = su_max_ts.dt
            LEFT JOIN su_forward_detail ON newconnect.connid = su_forward_detail.conn_id AND newconnect.dt = su_forward_detail.dt
            LEFT JOIN du_forward_detail ON newconnect.connid = du_forward_detail.conn_id AND newconnect.dt = du_forward_detail.dt
    ) t1
    GROUP BY dt, reconnect, type
    ORDER BY type
),
total_with_tot AS (
    SELECT
        dt,
        reconnect,
        type,
        c1,
        SUM(c1) OVER (PARTITION BY reconnect) AS tot
    FROM total
),
total_with_percentage AS (
    SELECT
        dt,
        reconnect,
        type,
        c1,
        tot,
        ROUND((c1 * 100.0 / tot), 2) AS percentage
    FROM total_with_tot
)
insert overwrite table ${APP}.ads_todesk_forward_failed_total
SELECT * FROM total_with_percentage
ORDER BY reconnect ASC, percentage DESC;"

ads_daas_user_gold_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_daas_user_gold_total
select
    '$do_date',
    case when gold >= 1 and gold <20*100 then '1'
         when gold >= 20*100 and gold <50*100 then '2'
         when gold >= 50*100 and gold <100*100 then '3'
         when gold >= 100*100 and gold <500*100 then '4'
         when gold >= 500*100  then '5' else 'other' end gold,count(1)
from ${APP}.ods_daas_tv_user
where dt='$do_date'
group by
    case when gold >= 1 and gold <20*100 then '1'
         when gold >= 20*100 and gold <50*100 then '2'
         when gold >= 50*100 and gold <100*100 then '3'
         when gold >= 100*100 and gold <500*100 then '4'
         when gold >= 500*100  then '5' else 'other' end;"

ads_daas_newuser_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_daas_newuser_total
select
    '$do_date',sclient,sum(if(tdid is not null,1,0)),count(1)
from
(
    select
        suid,case when sclient='1' then 'windows'
             when sclient='2' then 'mac'
             when sclient='3' then 'ios'
             when sclient='4' then 'android'
             when sclient='5' then 'linux'
             when sclient='6' then 'onther'
        else 'other' end sclient
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and labtype='daas'
    and sclient!='8' --web
    group by suid,case when sclient='1' then 'windows'
             when sclient='2' then 'mac'
             when sclient='3' then 'ios'
             when sclient='4' then 'android'
             when sclient='5' then 'linux'
             when sclient='6' then 'onther'
        else 'other' end
)t1
left join
(
    select
        tdid
    from ${APP}.dwd_new_user
    where dt='$do_date'
    and labtype='daas'
    group by tdid
)t2
on t1.suid=t2.tdid
group by sclient;"

ads_daas_user_distributed_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert into table ${APP}.ads_daas_user_distributed_total
SELECT
    '$do_date',
    SUM(IF(t2.userid IS null,1,0)) ,
    SUM(IF(t2.userid IS NOT null,1,0)) ,
    SUM(IF(t2.userid IS NOT null and t3.\`user\` IS NOT null,1,0)),
    COUNT(1)
FROM
( SELECT id FROM ${APP}.ods_daas_tv_user where dt='$do_date'
            and regdate>=unix_timestamp('$do_date 00:00:00')-28800
            and regdate<unix_timestamp(date_add('$do_date',1))-28800
) t1
LEFT JOIN
( SELECT userid from ${APP}.ods_daas_tv_todesk_user where dt='$do_date') t2
ON t1.id=t2.userid
LEFT JOIN
( SELECT \`user\` from ${APP}.dwd_user_login where dt='$do_date' and labtype='daas' group by \`user\`)  t3
ON t1.id=t3.\`user\`;"

ads_gaas_paying_user_total="
insert overwrite table ${APP}.ads_gaas_paying_user_total
select
    '$do_date',order_type,plat,sum(user_costtime),cast(sum(user_costtime)/count(1) as decimal(16,2)),count(1)
from
(
    select
        order_type,userid,case when client_plat='0' then 'other'
                 when client_plat='1' then 'windows'
                 when client_plat='2' then 'android'
                 when client_plat='3' then 'ios'
                 when client_plat='4' then 'linux'
                 when client_plat='5' then 'mac'
                 when client_plat='6' then 'web'
                 when client_plat='7' then 'unknown'
                 when client_plat='8' then 'MiniProgram'
                 else 'onther' end plat,sum(cast(costtime as bigint)) user_costtime
    from ${APP}.dwd_gass_consume
    where dt='$do_date'
    and env = '0'
    and !(order_type='0' and costtime='0')
    group by order_type,userid,case when client_plat='0' then 'other'
                 when client_plat='1' then 'windows'
                 when client_plat='2' then 'android'
                 when client_plat='3' then 'ios'
                 when client_plat='4' then 'linux'
                 when client_plat='5' then 'mac'
                 when client_plat='6' then 'web'
                 when client_plat='7' then 'unknown'
                 when client_plat='8' then 'MiniProgram'
                 else 'onther' end 
)t1
group by order_type,plat
union
select
    '$do_date',order_type,'all',sum(costtime),cast(sum(costtime)/count(distinct userid) as decimal(16,2)),count(distinct userid)
from ${APP}.dwd_gass_consume
where dt='$do_date'
and env = '0'
and !(order_type='0' and costtime='0')
group by order_type
union
select
    '$do_date','2',plat,0,0.00,count(1)
from
(
    select
	userid,case when agent_plat='0' then 'other'
                 when agent_plat='1' then 'windows'
                 when agent_plat='2' then 'android'
                 when agent_plat='3' then 'ios'
                 when agent_plat='4' then 'linux'
                 when agent_plat='5' then 'mac'
                 when agent_plat='6' then 'web'
                 when agent_plat='7' then 'unknown'
                 when agent_plat='8' then 'MiniProgram'
                 else 'onther' end plat
    from ${APP}.dwd_gass_gametab
    where dt='$do_date'
    and env='0' 
    group by userid,case when agent_plat='0' then 'other'
                 when agent_plat='1' then 'windows'
                 when agent_plat='2' then 'android'
                 when agent_plat='3' then 'ios'
                 when agent_plat='4' then 'linux'
                 when agent_plat='5' then 'mac'
                 when agent_plat='6' then 'web'
                 when agent_plat='7' then 'unknown'
                 when agent_plat='8' then 'MiniProgram'
                 else 'onther' end
)t1
group by plat
union
select
    '$do_date','2','all',0,0.00,count(distinct userid)
from ${APP}.dwd_gass_gametab
where dt='$do_date'
and env = '0';"

ads_gaas_free_user_retention="
insert overwrite table ${APP}.ads_gaas_free_user_retention
select
    '$do_date',count(1),date_sub('$do_date',6),7,sum(if(t2.userid is not null,1,0)),cast(100*sum(if(t2.userid is not null,1,0))/count(1) as decimal(16,2)),cast(sum(costmoney)/sum(if(t2.userid is not null,1,0))as decimal(16,2))
from
(
    select
        userid
    from ${APP}.dwd_gass_consume
    where dt = date_sub('$do_date',6)
    and order_type='1'
    and env='0'
    group by userid
)t1
left join
(
     select
        userid,sum(costmoney) costmoney
    from ${APP}.dwd_gass_consume
    where dt between date_sub('$do_date',6) and '$do_date'
    and order_type='0' and costtime!='0'
     and env='0'
    group by userid
)t2
on t1.userid=t2.userid
union
select
    '$do_date',count(1),date_sub('$do_date',30),31,sum(if(t2.userid is not null,1,0)),cast(100*sum(if(t2.userid is not null,1,0))/count(1) as decimal(16,2)),cast(sum(costmoney)/sum(if(t2.userid is not null,1,0))as decimal(16,2))
from
(
    select
        userid
    from ${APP}.dwd_gass_consume
    where dt = date_sub('$do_date',30)
    and order_type='1'
    and env='0'
    group by userid
)t1
left join
(
     select
        userid,sum(costmoney) costmoney
    from ${APP}.dwd_gass_consume
    where dt between date_sub('$do_date',30) and '$do_date'
    and order_type='0' and costtime!='0'
     and env='0'
    group by userid
)t2
on t1.userid=t2.userid;"

ads_gaas_paying_user_retention="
insert overwrite table ${APP}.ads_gaas_paying_user_retention
select
    '$do_date',count(1),date_sub('$do_date',7),7,sum(if(t2.userid is not null,1,0)),cast(100*sum(if(t2.userid is not null,1,0))/count(1) as decimal(16,2))
from
(
    select
        userid
    from ${APP}.dwd_gass_consume
    where dt = date_sub('$do_date',7)
    and order_type='0' and costtime!='0'
    and env='0'
    and newpay='1'
    group by userid
)t1
left join
(
     select
        userid
    from ${APP}.dwd_gass_consume
    where dt = '$do_date'
    and order_type='0' and costtime!='0'
     and env='0'
    group by userid
)t2
on t1.userid=t2.userid
union
select
    '$do_date',count(1),date_sub('$do_date',15),15,sum(if(t2.userid is not null,1,0)),cast(100*sum(if(t2.userid is not null,1,0))/count(1) as decimal(16,2))
from
(
    select
        userid
    from ${APP}.dwd_gass_consume
    where dt = date_sub('$do_date',15)
    and order_type='0' and costtime!='0'
    and env='0'
    and newpay='1'
    group by userid
)t1
left join
(
     select
        userid
    from ${APP}.dwd_gass_consume
    where dt = '$do_date'
    and order_type='0' and costtime!='0'
     and env='0'
    group by userid
)t2
on t1.userid=t2.userid
union
select
    '$do_date',count(1),date_sub('$do_date',31),31,sum(if(t2.userid is not null,1,0)),cast(100*sum(if(t2.userid is not null,1,0))/count(1) as decimal(16,2))
from
(
    select
        userid
    from ${APP}.dwd_gass_consume
    where dt = date_sub('$do_date',31)
    and order_type='0' and costtime!='0'
    and env='0'
    and newpay='1'
    group by userid
)t1
left join
(
     select
        userid
    from ${APP}.dwd_gass_consume
    where dt= '$do_date'
    and order_type='0' and costtime!='0'
     and env='0'
    group by userid
)t2
on t1.userid=t2.userid;"

ads_gaas_active_user_total="
insert overwrite table ${APP}.ads_gaas_active_user_total
select
   '$do_date','1',count(1)
from
(
    select
        user_id
    from ${APP}.dwd_gass_launchapp
    where dt='$do_date'
    and env='0'
    and new_user='1'
    group by user_id
)t1
union all
select
   '$do_date','0',count(1)
from
(
    select
        user_id
    from ${APP}.dwd_gass_launchapp
    where dt='$do_date'
    and env='0'
    group by user_id
)t1;"

ads_gaas_active_user_retention="
insert overwrite table ${APP}.ads_gaas_active_user_retention
select
    '$do_date',
    '0',7,
    c1,c2,cast(c1/c2 * 100 as decimal(16,2))
from
(
    select
        '$do_date' dt,count(1) c1
    from
    (
        select
            user_id
        from ${APP}.dwd_gass_launchapp
        where dt > date_sub('$do_date',7) and dt<= '$do_date'
        and env='0'
        group by user_id
    )t1
)t1
inner join
(
   select
        '$do_date' dt,count(1) c2
    from
    (
        select
            user_id
        from ${APP}.dwd_gass_launchapp
        where dt > date_sub('$do_date',14) and dt<= date_sub('$do_date',7)
        and env='0'
        group by user_id
    )t1
)t2
on t1.dt=t2.dt
union
select
    '$do_date',
    '1',31,
    c1,c2,cast(c1/c2 * 100 as decimal(16,2))
from
(
    select
        '$do_date' dt,count(1) c1
    from
    (
        select
            user_id
        from ${APP}.dwd_gass_launchapp
        where dt > date_sub('$do_date',31) and dt<= '$do_date'
        and env='0'
        and new_user='1'
        group by user_id
    )t1
)t1
inner join
(
   select
        '$do_date' dt,count(1) c2
    from
    (
        select
            user_id
        from ${APP}.dwd_gass_launchapp
        where dt > date_sub('$do_date',62) and dt<= date_sub('$do_date',31)
        and env='0'
        and new_user='1'
        group by user_id
    )t1
)t2
on t1.dt=t2.dt;"

ads_gaas_active_mac_total="
insert overwrite table ${APP}.ads_gaas_active_mac_total
select
    '$do_date',
     plat ,count(1)
from
(
    select
        client_id,plat
    from
    (
        select
            client_id,
            case when client_plat='0' then 'other'
                 when client_plat='1' then 'windows'
                 when client_plat='2' then 'android'
                 when client_plat='3' then 'ios'
                 when client_plat='4' then 'linux'
                 when client_plat='5' then 'mac'
                 when client_plat='6' then 'web'
                 when client_plat='7' then 'unknown'
                 when client_plat='8' then 'MiniProgram'
                 else 'onther' end plat
        from ${APP}.dwd_gass_launchapp
        where dt='$do_date'
        and env='0'
    )t1
    group by client_id,plat
)t1
group by plat;"

ads_cloud_new_user_total="
with tmp
as (
    select
        t1.id, 'daas' labtype,if(t2.plat is not null,t2.plat,'none') client
    from (
             SELECT id FROM ${APP}.ods_daas_tv_user where dt = '$do_date'
             and regdate>=unix_timestamp('$do_date 00:00:00')-28800
             and regdate<unix_timestamp(date_add('$do_date',1))-28800
             group by id
         ) t1
    left join
    (
        select
            \`user\`,plat
        from
         (
            SELECT \`user\`,case when client='0' then 'universal'
                         when client='1' then 'windows'
                         when client='2' then 'mac'
                         when client='3' then 'ios'
                         when client='4' then 'android'
                         when client='5' then 'linux'
                         when client='6' then 'onther'
                         when client='8' then 'web'
                         when client='9' then 'androidpad'
                         else 'onther' end plat,ts,
            min(ts) over(partition by \`user\`) min_ts
        from ${APP}.dwd_user_login
        where dt='$do_date' and labtype='daas' and client is not null
         )t1
        where ts=min_ts
        group by \`user\`,plat
    )t2
    on t1.id=t2.\`user\`
    union all
    select
        user_id,'cloudapp',plat
    from
    (
        select
            user_id,case when client_plat='0' then 'other'
                         when client_plat='1' then 'windows'
                         when client_plat='2' then 'android'
                         when client_plat='3' then 'ios'
                         when client_plat='4' then 'linux'
                         when client_plat='5' then 'mac'
                         when client_plat='6' then 'web'
                         when client_plat='7' then 'unknown'
                         when client_plat='8' then 'MiniProgram'
                         else 'onther' end plat,
            logtime,
            min(logtime) over(partition by user_id) min_ts
        from ${APP}.dwd_gass_launchapp
        where dt='$do_date'
        and env='0'
        and new_user='1'
    )t1
    where logtime=min_ts
    group by user_id,plat
    )
insert overwrite table ${APP}.ads_cloud_new_user_total
select
    '$do_date',
    labtype,client,
    SUM(IF(t2.userid IS null,1,0)),
    SUM(IF(t2.userid IS NOT null and t3.\`user\` IS NOT null,1,0)),
    SUM(IF(t2.userid IS null,1,0))+SUM(IF(t2.userid IS NOT null and t3.\`user\` IS NOT null,1,0))
from tmp
LEFT JOIN
( SELECT userid from ${APP}.ods_daas_tv_todesk_user where dt='$do_date') t2
ON tmp.id=t2.userid
LEFT JOIN
( SELECT \`user\` from ${APP}.dwd_user_login where dt='$do_date' and labtype='daas' group by \`user\`)  t3
ON tmp.id=t3.\`user\`
group by labtype,client;"



ads_cloud_paying_user_total="
with tmp as
(
    select
        t1.userid,'daas' labtype,t1.plat,if(t2.userid is not null,'1','0') is_renewal
    from
    (
        select
           cast(userid as string) userid,case when platform='windows' then 'windows'
                           when platform='mac' then 'mac'
                           when platform='iosApp' then 'ios'
                           when platform in ('androidApp','mobile') then 'android'
                           when platform='common' then 'web'
                           when platform='androidPad' then 'androidpad'
                            else 'onther' end plat
        from ${APP}.ods_daas_bill_order
        where dt='$do_date'
        and env=2 and status=2 and total_fee>0 and order_type>0
        and unix_timestamp(create_time)-28800>=unix_timestamp('$do_date 00:00:00')-28800
        and unix_timestamp(create_time)-28800<unix_timestamp(date_add('$do_date',1))-28800
        group by userid,case when platform='windows' then 'windows'
                           when platform='mac' then 'mac'
                           when platform='iosApp' then 'ios'
                           when platform in ('androidApp','mobile') then 'android'
                           when platform='common' then 'web'
                            when platform='androidPad' then 'androidpad'
                            else 'onther' end
    )t1
    left join
    (
        select
           userid
        from ${APP}.ods_daas_bill_order
        where dt='$do_date'
        and env=2 and status=2 and total_fee>0 and order_type>0
        and unix_timestamp(create_time)-28800<unix_timestamp('$do_date 00:00:00')-28800
        group by userid
    )t2
    on t1.userid=t2.userid
    union all
    select
        userid,'cloudapp',case when client_plat='0' then 'other'
                             when client_plat='1' then 'windows'
                             when client_plat='2' then 'android'
                             when client_plat='3' then 'ios'
                             when client_plat='4' then 'linux'
                             when client_plat='5' then 'mac'
                             when client_plat='6' then 'web'
                             when client_plat='7' then 'unknown'
                             when client_plat='8' then 'MiniProgram'
                             else 'onther' end plat,if(newpay='0','1','0') is_renewal
    from ${APP}.dwd_gass_consume
    where dt = '$do_date'
    and env='0'
    and (newpay='1' or (newpay='0' and costtime>0 and order_type='0'))
    group by userid,case when client_plat='0' then 'other'
                             when client_plat='1' then 'windows'
                             when client_plat='2' then 'android'
                             when client_plat='3' then 'ios'
                             when client_plat='4' then 'linux'
                             when client_plat='5' then 'mac'
                             when client_plat='6' then 'web'
                             when client_plat='7' then 'unknown'
                             when client_plat='8' then 'MiniProgram'
                             else 'onther' end ,if(newpay='0','1','0')
)
insert overwrite table ${APP}.ads_cloud_paying_user_total
select
    '$do_date',labtype,is_renewal,plat,
    SUM(IF(t2.userid IS null,1,0)),
    SUM(IF(t2.userid IS NOT null ,1,0)),
    SUM(IF(t2.userid IS null,1,0)) + SUM(IF(t2.userid IS NOT null ,1,0))
from tmp
LEFT JOIN
( SELECT userid from ${APP}.ods_daas_tv_todesk_user where dt='$do_date' group by userid ) t2
ON tmp.userid=t2.userid
group by labtype,is_renewal,plat
union all
select
    '$do_date','all',is_renewal,'all',
    SUM(IF(t2.userid IS null,1,0)),
    SUM(IF(t2.userid IS NOT null ,1,0)),
    SUM(IF(t2.userid IS null,1,0)) + SUM(IF(t2.userid IS NOT null ,1,0))
from
(
    select
        userid,is_renewal
    from tmp
    group by userid,is_renewal
)t1
LEFT JOIN
( SELECT userid from ${APP}.ods_daas_tv_todesk_user where dt='$do_date' group by userid) t2
ON t1.userid=t2.userid
group by is_renewal
union all
select
    '$do_date',labtype,is_renewal,'all',
    SUM(IF(t2.userid IS null,1,0)),
    SUM(IF(t2.userid IS NOT null ,1,0)),
    SUM(IF(t2.userid IS null,1,0)) + SUM(IF(t2.userid IS NOT null ,1,0))
from
(
    select
        userid,labtype,is_renewal
    from tmp
    group by userid,labtype,is_renewal
)t1
LEFT JOIN
( SELECT userid from ${APP}.ods_daas_tv_todesk_user where dt='$do_date' group by userid) t2
ON t1.userid=t2.userid
group by labtype,is_renewal
union all
select
    '$do_date','all',is_renewal,plat,
    SUM(IF(t2.userid IS null,1,0)),
    SUM(IF(t2.userid IS NOT null ,1,0)),
    SUM(IF(t2.userid IS null,1,0)) + SUM(IF(t2.userid IS NOT null ,1,0))
from
(
    select
        userid,plat,is_renewal
    from tmp
    group by userid,plat,is_renewal
)t1
LEFT JOIN
( SELECT userid from ${APP}.ods_daas_tv_todesk_user where dt='$do_date' group by userid) t2
ON t1.userid=t2.userid
group by is_renewal, plat;"


ads_cloud_connected_user_total="
with tmp as
(
    select
        'daas' labtype,flag,\`user\` userid,plat,sum(link_time) link_time
    from
    (
        select
            \`user\`,connid,case when resultmsg='gddxbmc' then '3060'
                               when resultmsg in ('btgz', 'btjh', 'btyc', 'btdy', 'btty', 'btjhb', 'btjhc', 'qshc', 'gzcm') then '4070'
                               when resultmsg in ('ucloud', 'gzhc') then '4090'
                        else 'other' end flag,case when sclient='0' then 'universal'
                         when sclient='1' then 'windows'
                         when sclient='2' then 'mac'
                         when sclient='3' then 'ios'
                         when sclient='4' then 'android'
                         when sclient='5' then 'linux'
                         when sclient='6' then 'onther'
                         when sclient='8' then 'web'
                         when sclient='9' then 'androidpad'
                         else 'onther' end plat
        from ${APP}.dwd_new_connect
        where dt between date_sub('$do_date',1) and '$do_date'
        and labtype='daas'
        group by \`user\`, connid,case when resultmsg='gddxbmc' then '3060'
                               when resultmsg in ('btgz', 'btjh', 'btyc', 'btdy', 'btty', 'btjhb', 'btjhc', 'qshc', 'gzcm') then '4070'
                               when resultmsg in ('ucloud', 'gzhc') then '4090'
                        else 'other' end,
                        case when sclient='0' then 'universal'
                         when sclient='1' then 'windows'
                         when sclient='2' then 'mac'
                         when sclient='3' then 'ios'
                         when sclient='4' then 'android'
                         when sclient='5' then 'linux'
                         when sclient='6' then 'onther'
                         when sclient='8' then 'web'
                         when sclient='9' then 'androidpad'
                         else 'onther' end
    )t1
    inner join
    (
         select
            connid,cast(link_time  as bigint) link_time
        from ${APP}.dwd_session_success
        where dt='$do_date'
        and labtype='daas'
        group by connid,link_time
    )t2
    on t1.connid=t2.connid
    group by flag,\`user\`,plat
    union all
    select
        'cloudapp','4070',cast(userid as string) userid,case when client_plat='0' then 'other'
                             when client_plat='1' then 'windows'
                             when client_plat='2' then 'android'
                             when client_plat='3' then 'ios'
                             when client_plat='4' then 'linux'
                             when client_plat='5' then 'mac'
                             when client_plat='6' then 'web'
                             when client_plat='7' then 'unknown'
                             when client_plat='8' then 'MiniProgram'
                             else 'onther' end plat,sum(cast(costtime as bigint)) costtime
    from ${APP}.dwd_gass_consume
    where dt='$do_date'
    and env='0'
    group by userid,case when client_plat='0' then 'other'
                             when client_plat='1' then 'windows'
                             when client_plat='2' then 'android'
                             when client_plat='3' then 'ios'
                             when client_plat='4' then 'linux'
                             when client_plat='5' then 'mac'
                             when client_plat='6' then 'web'
                             when client_plat='7' then 'unknown'
                             when client_plat='8' then 'MiniProgram'
                             else 'onther' end
)
insert overwrite table ${APP}.ads_cloud_connected_user_total
select
    '$do_date','all','all','all',sum(if(link_time between 3*60*60 and 24*60*60,1,0)),count(1)
from (
        select userid, sum(link_time) link_time
       from tmp
       group by userid
     )t1
union all
select
    '$do_date',labtype,flag,plat,sum(if(link_time between 3*60*60 and 24*60*60,1,0)),count(1)
from tmp
group by labtype, flag ,plat
union all
select
    '$do_date',labtype,'all','all',sum(if(link_time between 3*60*60 and 24*60*60,1,0)),count(1)
from (
        select labtype,userid, sum(link_time) link_time
       from tmp
       group by labtype,userid
     )t1
    group by labtype
union all
select
    '$do_date','all',flag,'all',sum(if(link_time between 3*60*60 and 24*60*60,1,0)),count(1)
from (
        select flag,userid, sum(link_time) link_time
       from tmp
       group by flag,userid
     )t1
group by flag
union all
select
    '$do_date','all','all',plat,sum(if(link_time between 3*60*60 and 24*60*60,1,0)),count(1)
from (
        select plat,userid, sum(link_time) link_time
       from tmp
       group by plat,userid
     )t1
group by plat
union all
select
    '$do_date',labtype,flag,'all',sum(if(link_time between 3*60*60 and 24*60*60,1,0)),count(1)
from (
        select labtype,userid,flag, sum(link_time) link_time
       from tmp
       group by labtype,userid,flag
     )t1
    group by labtype,flag
union all
select
    '$do_date','all',flag,plat,sum(if(link_time between 3*60*60 and 24*60*60,1,0)),count(1)
from (
        select flag,userid,plat, sum(link_time) link_time
       from tmp
       group by flag,userid,plat
     )t1
group by flag,plat
union all
select
    '$do_date',labtype,'all',plat,sum(if(link_time between 3*60*60 and 24*60*60,1,0)),count(1)
from (
        select labtype,userid,plat, sum(link_time) link_time
       from tmp
       group by labtype,userid,plat
     )t1
group by labtype,plat;"


ads_forward_hour_isp_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_forward_hour_isp_total
select
    '$do_date',
    'client',
    t1.*,t2.rate
from
(
    select
        hour,sisp,
        sum(if(t2.connid is not null ,1,0)) c1,
        count(1) c2,
        cast(sum(if(t2.connid is not null , 1,0)) /count(t1.connid) * 100 as decimal(16,2)) rate1
    from
    (
        select connid,case when sisp='联通' then '联通'
                 when sisp='电信' then '电信'
                 when sisp='移动' then '移动'
                 else '其他' end sisp,date_format(from_utc_timestamp(ts,'Asia/Shanghai'),'HH') hour
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        and labtype !='daas'
        and reconnect='0'
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
    group by hour,sisp
)t1
inner join
(
    select
        hour,sisp,
        sum(if(t2.connid is not null ,1,0)) c1,
        count(1) c2,
        cast(sum(if(t2.connid is not null , 1,0)) /count(t1.connid) * 100 as decimal(16,2))  rate
    from
    (
        select connid,case when sisp='联通' then '联通'
                 when sisp='电信' then '电信'
                 when sisp='移动' then '移动'
                 else '其他' end sisp,date_format(from_utc_timestamp(ts,'Asia/Shanghai'),'HH') hour
        from ${APP}.dwd_new_connect
        where dt=date_sub('$do_date',7)
        and labtype !='daas'
        and reconnect='0'
    )t1
    left join
    (
        select
               connid
        from ${APP}.dwd_handshake_success
        where dt=date_sub('$do_date',7)
        group by connid
 	   )t2
    on t1.connid=t2.connid
    group by hour,sisp
)t2
on t1.hour=t2.hour and t1.sisp=t2.sisp
where t2.rate-t1.rate1>1
union all
select
    '$do_date',
        'host',
    t1.*,t2.rate
from
(
    select
        hour,disp,
        sum(if(t2.connid is not null ,1,0)) c1,
        count(1) c2,
        cast(sum(if(t2.connid is not null , 1,0)) /count(t1.connid) * 100 as decimal(16,2)) rate1
    from
    (
        select connid,case when disp='联通' then '联通'
                 when disp='电信' then '电信'
                 when disp='移动' then '移动'
                 else '其他' end disp,date_format(from_utc_timestamp(ts,'Asia/Shanghai'),'HH') hour
        from ${APP}.dwd_new_connect
        where dt='$do_date'
        and labtype !='daas'
        and reconnect='0'
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
    group by hour,disp
)t1
inner join
(
    select
        hour,disp,
        sum(if(t2.connid is not null ,1,0)) c1,
        count(1) c2,
        cast(sum(if(t2.connid is not null , 1,0)) /count(t1.connid) * 100 as decimal(16,2)) rate
    from
    (
        select connid,case when disp='联通' then '联通'
                 when disp='电信' then '电信'
                 when disp='移动' then '移动'
                 else '其他' end disp,date_format(from_utc_timestamp(ts,'Asia/Shanghai'),'HH') hour
        from ${APP}.dwd_new_connect
        where dt=date_sub('$do_date',7)
        and labtype !='daas'
        and reconnect='0'
    )t1
    left join
    (
        select
               connid
        from ${APP}.dwd_handshake_success
        where dt=date_sub('$do_date',7)
        group by connid
    )t2
    on t1.connid=t2.connid
    group by hour,disp
)t2
on t1.hour=t2.hour and t1.disp=t2.disp
where t2.rate-t1.rate1>1;"

ads_ec_hour_isp_total="
insert overwrite table ${APP}.ads_ec_hour_isp_total
select
    '$do_date',
    t1.*,t2.rate
from
(
    SELECT
       clientype,
       date_format(from_utc_timestamp(logtime,'Asia/Shanghai'),'HH') hour,
       case when network='联通' then '联通'
             when network='电信' then '电信'
             when network='移动' then '移动'
             else '其他' end network,
       sum(if(turndata_connected='1',1,0)),
       count(1),
       cast(sum(if(turndata_connected='1',1,0))/count(1) *100 as decimal(16,2)) rate1
    FROM
       ${APP}.dwd_licode_sig
    WHERE
       dt = '$do_date'
    GROUP BY
       clientype, date_format(from_utc_timestamp(logtime,'Asia/Shanghai'),'HH') , case when network='联通' then '联通'
             when network='电信' then '电信'
             when network='移动' then '移动'
             else '其他' end
)t1
inner join
(
    SELECT
       clientype,
       date_format(from_utc_timestamp(logtime,'Asia/Shanghai'),'HH') hour,
       case when network='联通' then '联通'
             when network='电信' then '电信'
             when network='移动' then '移动'
             else '其他' end network,
       sum(if(turndata_connected='1',1,0)),
       count(1),
       cast(sum(if(turndata_connected='1',1,0))/count(1) *100 as decimal(16,2)) rate
    FROM
       ${APP}.dwd_licode_sig
    where dt=date_sub('$do_date',7)
    GROUP BY
       clientype, date_format(from_utc_timestamp(logtime,'Asia/Shanghai'),'HH') , case when network='联通' then '联通'
             when network='电信' then '电信'
             when network='移动' then '移动'
             else '其他' end
)t2
on t1.hour=t2.hour and t1.network=t2.network and t1.clientype=t2.clientype
where t2.rate-t1.rate1>1;"

ads_ea_hour_isp_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_ea_hour_isp_total
select
    '$do_date',
    '',t1.hour,'',a,b,rate1
from
(
    select
        
        hour,
        sum(if(t3.streamid is  null and  t4.streamid is not null  ,1,0)) a,
        sum(if(t3.streamid is  null,1,0)) b,
        cast(sum(if(t3.streamid is  null and  t4.streamid is not null ,1,0))/sum(if(t3.streamid is  null ,1,0)) * 100 as decimal(16,2)) rate1
    from
    (
        select
            t1.*
        from
        (
            select
                streamid,is_sfu,date_format(from_utc_timestamp(logtime,'Asia/Shanghai'),'HH') hour,count(1) c1
            from ${APP}.dwd_backend_licode_subscribe_sfu
            where dt='$do_date'
            group by streamid,is_sfu,date_format(from_utc_timestamp(logtime,'Asia/Shanghai'),'HH')
        )t1
        inner join
        (
            select
                stream_id
            from ${APP}.dwd_backend_licode_sdk_report
            where dt='$do_date' and reason != '1'
            group by stream_id
        )t2
        on t1.streamid=t2.stream_id
    )t1
    left join
    (
        select
            streamid
        from ${APP}.dwd_backend_licode_subscribe_p2p
        where dt='$do_date'
        group by streamid
    )t3
    on t1.streamid=t3.streamid
    left join
    (
        select
            streamid
        from ${APP}.dwd_backend_licode_subscribe_ready
        where dt='$do_date'
        group by streamid
    )t4
    on t1.streamid=t4.streamid
    where is_sfu='false' and c1=1
    group by hour
)t1;"

ads_windows_tfa_total="
insert overwrite table ${APP}.ads_windows_tfa_total
select
    '$do_date',action, flag,count(1)
from
(
    select
        action,case
                    when   action='OpenTfaForConn' and get_json_object(ck_url,'$.TfaAgree') is not null then 1
                    when   action='OpenTfaForConn' and get_json_object(ck_url,'$.TfaReject') is not null then 2
                    when   action='OpenTfaForConn' and get_json_object(ck_url,'$.TfaTimeOut') is not null then 3
                    when   action='OpenTfaForConn' and get_json_object(ck_url,'$.TfaWaitCancel') is not null then 4
                    when   action='CloseTfaForConn' and get_json_object(ck_url,'$.TfaAgree') is not null then 1
                    when   action='CloseTfaForConn' and get_json_object(ck_url,'$.TfaReject') is not null then 2
                    when   action='CloseTfaForConn' and get_json_object(ck_url,'$.TfaTimeOut') is not null then 3
                    when   action='CloseTfaForConn' and get_json_object(ck_url,'$.TfaWaitCancel') is not null then 4
                    when   action='TfaConn' and get_json_object(ck_url,'$.TfaConnAgree') is not null then 1
                    when   action='TfaConn' and get_json_object(ck_url,'$.TfaConnReject') is not null then 2
                    when   action='TfaConn' and get_json_object(ck_url,'$.TfaConnTimeOut') is not null then 3
                    when   action='TfaConn' and get_json_object(ck_url,'$.TfaConnWaitCancel') is not null then 4
                    else 0 end flag
    from ${APP}.dwd_client_event
    where dt='$do_date'
    and action in ('TfaConn','OpenTfaForConn','CloseTfaForConn')
)t1
group by action,flag;"

ads_sec_verif_total="
insert overwrite table ${APP}.ads_sec_verif_total
select
    '$do_date',mode,if(flag is null,'nodata',flag),count(1)
from
(
    select
        mode,token
    from ${APP}.dwd_newconnect_auth
    where dt='$do_date'
    and type='4'
    group by mode,token
)t1
left join
(
    select
        token,flag
    from
    (
        select
            token,min(logtime) over(partition by token) min_ts,flag,logtime
        from
         (
            select
                token,case
                        when type='0' then 'unknown_auth'
                        when type='1' then 'agree'
                        when type='2' then 'reject'
                        when type='3' then 'wait_cancel'
                        else 'other' end flag,logtime
            from ${APP}.dwd_newconnect_auth
            where dt='$do_date'
            and type in ('0','1','2','3')
            union all
            select
                token,case
                        when type='0' then concat_ws('.','unknown_auth',resultmsg)
                        when type='1' then concat_ws('.','agree',resultmsg)
                        when type='2' then concat_ws('.','reject',resultmsg)
                        when type='3' then concat_ws('.','wait_cancel',resultmsg)
                        else 'other' end flag,logtime
            from ${APP}.dwd_newconnect_auth_fail
            where dt='$do_date'
         )t1
    )t2
    where min_ts=logtime
)t2
on t1.token=t2.token
group by mode,flag;"

ads_cross_provincial_total="
insert overwrite table ${APP}.ads_cross_provincial_total 
select
    '$do_date',
    is_relay,
    sum(if(handshake_success_connid is not null,1,0)) handshake_success_count,
    sum(if(authentication_success_connid is not null,1,0)) authentication_success_count,
    count(1) new_connect_count,
    cast(sum(if(handshake_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) handshake_success_rate,
    cast(sum(if(authentication_success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) authentication_success_rate
from
(
    select
           connid,
           if(sprovince=dprovince,'0','1') is_relay,
           sprovince,dprovince
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and labtype='excalibur'
    and dprovince in ('广东省','江苏省','上海市','浙江省','福建省','山东省','河南省','四川省','北京市','河北省','辽宁省','吉林省','湖南省','安徽省','湖北省','江西省','广西壮族自治区','黑龙江省','陕西省','山西省','重庆市','贵州省','天津市','云南省','内蒙古自治区','新疆维吾尔自治区','甘肃省','中国香港','海南省','宁夏回族自治区','中国台湾','青海省','西藏自治区','澳门特别行政区')
    and sprovince in ('广东省','江苏省','上海市','浙江省','福建省','山东省','河南省','四川省','北京市','河北省','辽宁省','吉林省','湖南省','安徽省','湖北省','江西省','广西壮族自治区','黑龙江省','陕西省','山西省','重庆市','贵州省','天津市','云南省','内蒙古自治区','新疆维吾尔自治区','甘肃省','中国香港','海南省','宁夏回族自治区','中国台湾','青海省','西藏自治区','澳门特别行政区')
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
)t4
on t1.connid=t4.handshake_success_connid
group by is_relay;"

ads_gaas_game_total="
insert overwrite table ${APP}.ads_gaas_game_total
select
    '$do_date',appname,count(1),sum(c1),cast(sum(c1)/count(1) as decimal(16,2)),sum(costtime1),cast(sum(costtime1)/count(1) as decimal(16,2))
from
(
    select
        appname,userid,count(1) c1,sum(costtime) costtime1
    from ${APP}.dwd_gass_consume
    where dt ='$do_date'
    and order_type='0' and costtime!='0'
    and env='0'
    and appname is not null
    group by appname,userid
)t1
group by appname;"

ads_dass_connect_fail_total="
insert overwrite table ${APP}.ads_dass_connect_fail_total
select
    '$do_date','su',*
from
(
    select
        suid, handshake_success_count, handshake_fail_count, new_connect_count, session_rate,cast(handshake_fail_count /sum(handshake_fail_count) over ()  * 100 as decimal(16,2))
    from
    (
        select
            suid,
            sum(if(success_connid is not null,1,0)) handshake_success_count,
            sum(if(success_connid is  null,1,0)) handshake_fail_count,
            count(1) new_connect_count,
            cast(sum(if(success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) session_rate
        from
        (
            select connid,
                   suid
            from ${APP}.dwd_new_connect
            where dt='$do_date'
            and labtype='daas'
        )t1
        left join
        (
            select
                   connid success_connid
            from ${APP}.dwd_handshake_success
            where dt='$do_date'
            group by connid
        )t2
        on t1.connid=t2.success_connid
        group by suid
    )t1
    order by handshake_fail_count desc
    limit 10
)t1
union all
select '$do_date','du',*
from
(
    select
        duid, handshake_success_count, handshake_fail_count, new_connect_count, session_rate,cast(handshake_fail_count /sum(handshake_fail_count) over ()  * 100 as decimal(16,2))
    from
    (
        select
            duid,
            sum(if(success_connid is not null,1,0)) handshake_success_count,
            sum(if(success_connid is  null,1,0)) handshake_fail_count,
            count(1) new_connect_count,
            cast(sum(if(success_connid is not null,1,0)) /count(1) * 100 as decimal(16,2)) session_rate
        from
        (
            select connid,
                   duid
            from ${APP}.dwd_new_connect
            where dt='$do_date'
            and labtype='daas'
        )t1
        left join
        (
            select
                   connid success_connid
            from ${APP}.dwd_handshake_success
            where dt='$do_date'
            group by connid
        )t2
        on t1.connid=t2.success_connid
        group by duid
    )t1
    order by handshake_fail_count desc
    limit 10
)t2;"

ads_limit_user_total="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.ads_limit_user_total
select
    '$do_date',flag,t1.id
from
(
    select
        t1.id
    from
    (
        select
            id
        from ${APP}.dim_user_info
        where dt ='$do_date'
        and create_time<unix_timestamp(date_sub('$do_date',6))-28800
        group by id
    )t1
    left join
    (
        select
            user_id
        from ${APP}.dwd_tv_function_config
        where dt ='$do_date'
        and status='1'
        and end_time>unix_timestamp(date_add('$do_date',1))-28800
        group by user_id
    )t2
    on t1.id=t2.user_id
    where user_id is null
)t1
inner join
(
    select
        1 flag,\`user\`
    from
    (
        select
            \`user\`,count(1) c1
        from ${APP}.dwd_new_connect
        where dt>=date_sub('$do_date',2) and dt<='$do_date'
        and enterprise='0'
        and labtype='excalibur'
        group by \`user\`
    )t1
    where c1>30
    union
    select
        2,\`user\`
    from
    (
        select
            \`user\`,count(1) c1
        from
        (
            select
                \`user\`,duser
            from ${APP}.dwd_new_connect
            where dt>=date_sub('$do_date',2) and dt<='$do_date'
            and enterprise='0'
            and labtype='excalibur'
            group by \`user\`,duser
        )t1
        group by \`user\`
    )t1
    where c1>3
    union
    select
        flag,t1.id
    from
    (
        select
            id,phone_num
        from ${APP}.dim_user_info
        where dt ='$do_date'
        and create_time<unix_timestamp(date_sub('$do_date',6))-28800
        group by id,phone_num
    )t1
    inner join
    (
        select
            3 flag,\`user\`
        from ${APP}.dwd_session_success
        where dt>=date_sub('$do_date',2) and dt<='$do_date'
        and enterprise='0'
        and link_time > 11*60*60
        group by \`user\`
        union
        select
            4,\`user\`
        from
        (
            select
                \`user\`,sum(link_time) time1
            from ${APP}.dwd_session_success
            where dt>=date_sub('$do_date',2) and dt<='$do_date'
            and enterprise='0'
            group by \`user\`
        )t1
        where time1 > 24*60*60
    )t2
    on t1.phone_num=t2.\`user\`
)t2
on t1.id=t2.\`user\`
union
select
    '$do_date',flag,t1.id
from
(
    select
        t1.id
    from
    (
        select
            id
        from ${APP}.dim_user_info
        where dt ='$do_date'
        and create_time<unix_timestamp(date_sub('$do_date',6))-28800
        group by id
    )t1
    left join
    (
        select
            user_id
        from ${APP}.dwd_tv_function_config
        where dt ='$do_date'
        and status='1'
        and end_time>unix_timestamp(date_add('$do_date',1))-28800
        group by user_id
    )t2
    on t1.id=t2.user_id
    where user_id is null
)t1
inner join
(
    select
        5 flag,\`user\`
    from
    (
        select
            \`user\`,count(1) c1
        from ${APP}.dwd_new_connect
        where dt>=date_sub('$do_date',6) and dt<='$do_date'
        and enterprise='0'
        and labtype='excalibur'
        group by \`user\`
    )t1
    where c1>50
    union
    select
        6,\`user\`
    from
    (
        select
            \`user\`,count(1) c1
        from
        (
            select
                \`user\`,duser
            from ${APP}.dwd_new_connect
            where dt>=date_sub('$do_date',6) and dt<='$do_date'
            and enterprise='0'
            and labtype='excalibur'
            group by \`user\`,duser
        )t1
        group by \`user\`
    )t1
    where c1>6
    union
    select
        flag,t1.id
    from
    (
        select
            id,phone_num
        from ${APP}.dim_user_info
        where dt ='$do_date'
        and create_time<unix_timestamp(date_sub('$do_date',6))-28800
        group by id,phone_num
    )t1
    inner join
    (
        select
            7 flag, \`user\`
        from ${APP}.dwd_session_success
        where dt>=date_sub('$do_date',6) and dt<='$do_date'
        and enterprise='0'
        and link_time > 11*60*60
        group by \`user\`
        union
        select
            8,\`user\`
        from
        (
            select
                \`user\`,sum(link_time) time1
            from ${APP}.dwd_session_success
            where dt>=date_sub('$do_date',6) and dt<='$do_date'
            and enterprise='0'
            group by \`user\`
        )t1
        where time1 > 30*60*60
    )t2
    on t1.phone_num=t2.\`user\`
)t2
on t1.id=t2.\`user\`;"


ads_limit_tpy_total="
insert overwrite table ${APP}.ads_limit_tpy_total
select
    '$do_date',t1.duid
from
(
    select
        duid
    from ${APP}.dwd_new_connect
    where dt='$do_date'
    and enterprise='0'
    and labtype='excalibur'
    group by duid
)t1
inner join
(
    select
        mac_id
    from ${APP}.dwd_client_event
    where dt='$do_date'
    AND ((cpu LIKE '%E5-26%' and memory in ('15.9G','63.9G','47.9G','127.9G'))
        or (cpu LIKE '%E5-26%' and memory in ('15.9G','63.9G','47.9G','127.9G') and substr(city,0,3) in ('四川省','黑龙江','贵州省','湖北省','浙江省','广东省','山西省'))
        or (ip in ('111.75.254.196','111.75.254.200')))
    group by mac_id
)t2
on t1.duid=t2.mac_id;"

case $1 in
    ads_session_day )
        hive -e "$ads_session_day"
    ;;
    ads_conn_duration )
        hive -e "$ads_conn_duration"
    ;;
    ads_su_client_version_total )
        hive -e "$ads_su_client_version_total"
    ;;
    ads_du_client_version_total )
        hive -e "$ads_du_client_version_total"
    ;;
    ads_client_connect )
        hive -e "$ads_client_connect"
    ;;
    ads_client_closed )
        hive -e "$ads_client_closed"
    ;;
    ads_user_logout )
        hive -e "$ads_user_logout"
    ;;
    ads_add_device )
        hive -e "$ads_add_device"
    ;; 
    ads_delete_device )
        hive -e "$ads_delete_device"
    ;;
    ads_edit_device )
        hive -e "$ads_edit_device"
    ;;
    ads_add_group )
        hive -e "$ads_add_group"
    ;;
    ads_delete_group )
        hive -e "$ads_delete_group"
    ;;
    ads_edit_group )
        hive -e "$ads_edit_group"
    ;;
    ads_android_logic_total )
        hive -e "$ads_android_logic_total"
    ;;
    ads_android_connect_total )
        hive -e "$ads_android_connect_total"
    ;;
    ads_phone_new_user )
        hive -e "$ads_phone_new_user"
    ;;
    ads_android_su_action )
        hive -e "$ads_android_su_action"
    ;;
    ads_android_bus )
        hive -e "$ads_android_bus"
    ;;
    ads_android_resolution )
        hive -e "$ads_android_resolution"
    ;;
    ads_android_bios )
        hive -e "$ads_android_bios"
    ;;
    ads_android_signin_total ) 
        hive -e "$ads_android_signin_total"
    ;;
    ads_sclient_session )
        hive -e "$ads_sclient_session"
    ;;
    ads_phone_user_version )
        hive -e "$ads_phone_user_version"
    ;;
    ads_rtc_conn_total )
        hive -e "$ads_rtc_conn_total"
    ;;
    ads_rtc_vedio_total )
        hive -e "$ads_rtc_vedio_total"
    ;;
    ads_rtc_conn_fst_time )
        hive -e "$ads_rtc_conn_fst_time"
    ;;
    ads_rtc_cap_send )
        hive -e "$ads_rtc_cap_send"
    ;;
    ads_rtc_rcvd_rend )
        hive -e "$ads_rtc_rcvd_rend"
    ;;
    ads_rtc_du_rate )
        hive -e "$ads_rtc_du_rate"
    ;;
    ads_rtc_rtt )
        hive -e "$ads_rtc_rtt"
    ;;
    ads_rtc_loss )
        hive -e "$ads_rtc_loss"
    ;;
    ads_bus_new_user )
        hive -e "$ads_bus_new_user"
    ;;
    ads_bus_user_start )
        hive -e "$ads_bus_user_start"
    ;;
    ads_bus_user_retention )
        hive -e "$ads_bus_user_retention"
    ;;
    ads_new_user )
        hive -e "$ads_new_user"
    ;;
    ads_start_user )
        hive -e "$ads_start_user"
    ;;
    ads_user_retention )
        hive -e "$ads_user_retention"
    ;;
    ads_client_new_mac )
        hive -e "$ads_client_new_mac"
    ;;
    ads_client_start_mac )
        hive -e "$ads_client_start_mac"
    ;;
    ads_mac_retention )
        hive -e "$ads_mac_retention"
    ;;
    ads_client_new_su )
        hive -e "$ads_client_new_su"
    ;;
    ads_client_start_su )
        hive -e "$ads_client_start_su"
    ;;
    ads_su_retention )
        hive -e "$ads_su_retention"
    ;;
    ads_client_new_du )
        hive -e "$ads_client_new_du"
    ;;
    ads_client_start_du )
        hive -e "$ads_client_start_du"
    ;;
    ads_du_retention )
        hive -e "$ads_du_retention"
    ;;
    ads_version_mac_total )
        hive -e "$ads_version_mac_total"
    ;;
    ads_su_handshake_total )
        hive -e "$ads_su_handshake_total"
    ;;
    ads_du_handshake_total ) 
        hive -e "$ads_du_handshake_total"
    ;;
    ads_su_handshake_failure )
        hive -e "$ads_su_handshake_failure"
    ;;
    ads_du_handshake_failure )
        hive -e "$ads_du_handshake_failure"
    ;;
    ads_user_login_total )
	hive -e "$ads_user_login_total"
    ;;
    ads_vendor_process_total )
	hive -e "$ads_vendor_process_total"
    ;;
    ads_windows_first_frame_total )
	hive -e "$ads_windows_first_frame_total"
    ;;
    ads_android_action_total )
	hive -e "$ads_android_action_total"
    ;;
    ads_android_action_pvuv )
	hive -e "$ads_android_action_pvuv"
    ;;
    ads_android_du_total )
	hive -e "$ads_android_du_total"
    ;;
    ads_ip_user_login_total )
	hive -e "$ads_ip_user_login_total"
    ;;
    ads_android_setting_total )
	hive -e "$ads_android_setting_total"
    ;;
    ads_android_test_double )
	hive -e "$ads_android_test_double"
    ;;
    ads_client_version_retention )
	hive -e "$ads_client_version_retention"
    ;;
    ads_client_version_new_su )
	hive -e "$ads_client_version_new_su"
    ;;
    ads_client_version_du_total )
	hive -e "$ads_client_version_du_total"
    ;;
    ads_client_version_du_retention )
	hive -e "$ads_client_version_du_retention"
    ;;
    ads_client_logic_total )
	hive -e "$ads_client_logic_total"
    ;;
    ads_client_logic_retention )
	hive -e "$ads_client_logic_retention"
    ;;
    ads_rtc_version_conn_total )
	hive -e "$ads_rtc_version_conn_total"
    ;;
	ads_rtc_version_vedio_total )
	hive -e "$ads_rtc_version_vedio_total"
	;;
	ads_rtc_version_fallback_total )
	hive -e "$ads_rtc_version_fallback_total"
	;;
	ads_rtc_version_conn_fst_time )
	hive -e "$ads_rtc_version_conn_fst_time"
	;;
	ads_rtc_version_cap_send )
	hive -e "$ads_rtc_version_cap_send"
	;;
	ads_rtc_version_rcvd_rend )
	hive -e "$ads_rtc_version_rcvd_rend"
	;;
	ads_rtc_version_du_rate )
	hive -e "$ads_rtc_version_du_rate"
	;;
	ads_rtc_version_rtt )
	hive -e "$ads_rtc_version_rtt"
	;;
	ads_rtc_version_loss )
	hive -e "$ads_rtc_version_loss"
	;;
	ads_android_login_total )
	hive -e "$ads_android_login_total"
	;;
	ads_licode_session_day )
	hive -e "$ads_licode_session_day"
	;;
	ads_client_licode_session_day )
	hive -e "$ads_client_licode_session_day"
	;;
	ads_web_setting_total )
	hive -e "$ads_web_setting_total"
	;;
	ads_su_province_total )
	hive -e "$ads_su_province_total"
	;;
        ads_android_licode_total )
	hive -e "$ads_android_licode_total"
        ;;
        ads_ios_licode_total )
	hive -e "$ads_ios_licode_total"
        ;;
	ads_licode_sdk_total )
	hive -e "$ads_licode_sdk_total"
	;;
	ads_licode_sig_total )
	hive -e "$ads_licode_sig_total"
	;;
	ads_licode_ice_total )
	hive -e "$ads_licode_ice_total"
	;;
	ads_android_link_total )
	hive -e "$ads_android_link_total"
	;;
	ads_android_forward_total )
	hive -e "$ads_android_forward_total"
	;;
	ads_ios_login_total )
	hive -e "$ads_ios_login_total"
	;;
	ads_windows_first_frame_fail )
	hive -e "$ads_windows_first_frame_fail"
	;;
	ads_windows_first_frame_success )
	hive -e "$ads_windows_first_frame_success"
	;;
	ads_android_first_frame_total )
	hive -e "$ads_android_first_frame_total"
;;
ads_android_vedio_total_track )
	hive -e "$ads_android_vedio_total_track"
;;
ads_android_type_vedio_total_rtc )
	hive -e "$ads_android_type_vedio_total_rtc"
;;
ads_android_vedio_total_rtc )
	hive -e "$ads_android_vedio_total_rtc"
;;
ads_android_decoder_total )
	hive -e "$ads_android_decoder_total"
;;
ads_android_cap_send_total )
	hive -e "$ads_android_cap_send_total"
;;
ads_licode_conn_time_total )
	hive -e "$ads_licode_conn_time_total"
;;
ads_version_new_connect_total )
	hive -e "$ads_version_new_connect_total"
;;
ads_version_new_connect_clientype_total )
	hive -e "$ads_version_new_connect_clientype_total"
;;
ads_backend_licode_loss_total )
	hive -e "$ads_backend_licode_loss_total"
;;
ads_windows_clarity_total )
	hive -e "$ads_windows_clarity_total" 
;;
ads_id_request_phone_total )
        hive -e "$ads_id_request_phone_total"
;;
ads_version_new_connect_type_total )
	hive -e "$ads_version_new_connect_type_total"
;;
ads_android_forward_fallback_total )
	hive -e "$ads_android_forward_fallback_total"
;;
ads_android_logic_topic_total )
	hive -e "$ads_android_logic_topic_total"
;;
ads_android_logic_retention )
	hive -e "$ads_android_logic_retention"
;;
ads_connection_total )
	hive -e "$ads_connection_total"
;;
ads_p2p_network_total )
	hive -e "$ads_p2p_network_total"
;;
ads_licode_connection_action )
	hive -e "$ads_licode_connection_action"
;;
ads_backend_ec_error_reason_all )
	hive -e "$ads_backend_ec_error_reason_all"
;;
ads_backend_ec_error_reason_both )
	hive -e "$ads_backend_ec_error_reason_both"
;;
ads_rtc_switch_total )
        hive -e "$ads_rtc_switch_total"
;;
ads_backend_ea_error_reason_all )
	hive -e "$ads_backend_ea_error_reason_all"
;;
ads_backend_ea_error_reason_both )
	hive -e "$ads_backend_ea_error_reason_both"
;;
ads_serv_diff_total )
	hive -e "$ads_serv_diff_total"
;;
ads_reconnect_session_day )
	hive -e "$ads_reconnect_session_day"
;;
ads_client_newuser_newsu )
 hive -e "$ads_client_newuser_newsu"
 ;;
ads_client_newuser_suretention )
 hive -e "$ads_client_newuser_suretention"
 ;;
ads_client_newuser_newdu )
 hive -e "$ads_client_newuser_newdu"
 ;;
ads_client_newuser_duretention )
 hive -e "$ads_client_newuser_duretention"
 ;;
ads_android_du_cap_send_total )
	hive -e "$ads_android_du_cap_send_total"
;;
ads_bigdata_datas_total )
	hive -e "$ads_bigdata_datas_total"
;;
ads_rtc_rpru_total )
	hive -e "$ads_rtc_rpru_total"
;;
ads_pc_new_version_total )
	hive -e "$ads_pc_new_version_total"
;;
ads_licode_connection_action_4500 )
    hive -e "$ads_licode_connection_action_4500"
;;
ads_backend_ec_error_reason_all_4500 )
    hive -e "$ads_backend_ec_error_reason_all_4500"
;;
ads_backend_ec_error_reason_both_4500 )
    hive -e "$ads_backend_ec_error_reason_both_4500"
;;
ads_backend_ea_error_reason_all_4500 )
    hive -e "$ads_backend_ea_error_reason_all_4500"
;;
ads_backend_ea_error_reason_both_4500 )
    hive -e "$ads_backend_ea_error_reason_both_4500"
;;
ads_licode_connection_stream_4500 )
    hive -e "$ads_licode_connection_stream_4500"
;;
ads_serv_recover_total )
	hive -e "$ads_serv_recover_total"
;;
ads_product_banner_total )
	hive -e "$ads_product_banner_total"
;;
ads_mac_avg_total )
	hive -e "$ads_mac_avg_total"
;;
ads_serv_all_not_present_total )
	hive -e "$ads_serv_all_not_present_total"
;;
dwd_anti_fraud_data )
	hive -e "$dwd_anti_fraud_data"
;;
dwd_anti_fraud_order )
	hive -e "$dwd_anti_fraud_order"
;;
ads_licode_sfu_total )
	hive -e "$ads_licode_sfu_total"
;;
ads_mobile_vedio_total )
	hive -e "$ads_mobile_vedio_total"
;;
ads_mobile_conn_fst_time_total )
	hive -e "$ads_mobile_conn_fst_time_total"
;;
ads_mobile_dec_total )
	hive -e "$ads_mobile_dec_total"
;;
ads_mobile_enc_total )
	hive -e "$ads_mobile_enc_total"
;;
ads_mobile_rtc_cap_rate )
	hive -e "$ads_mobile_rtc_cap_rate"
;;
ads_mobile_rtc_tar_rate )
	hive -e "$ads_mobile_rtc_tar_rate"
;;
ads_mobile_rtc_rtt )
	hive -e "$ads_mobile_rtc_rtt"
;;
ads_mobile_rtc_loss )
	hive -e "$ads_mobile_rtc_loss"
;;
ads_su_country_total )
	hive -e "$ads_su_country_total"
;;
ads_du_country_total )
	hive -e "$ads_du_country_total"
;;
ads_bad_serv_top )
	hive -e "$ads_bad_serv_top"
;;
ads_tob_bandwidth_total )
	hive -e "$ads_tob_bandwidth_total"
;;
ads_vip_conn_duration )
	hive -e "$ads_vip_conn_duration"
;;
ads_process_session_day )
	hive -e "$ads_process_session_day"
;;
ads_client_activesu )
	hive -e "$ads_client_activesu"
;;
ads_client_activedu )
	hive -e "$ads_client_activedu"
;;
ads_operation_total )
	hive -e "$ads_operation_total"
;;
ads_file_transport_total )
	hive -e "$ads_file_transport_total"
;;
ads_daas_handshake )
	hive -e "$ads_daas_handshake"
;;
ads_connect_limit_total )
	hive -e "$ads_connect_limit_total"
;;
ads_daas_licode_handshake )
	hive -e "$ads_daas_licode_handshake"
;;
ads_daas_selling_time_total )
	hive -e "$ads_daas_selling_time_total"
;;
ads_daas_used_time_total )
	hive -e "$ads_daas_used_time_total"
;;
ads_daas_user_total )
	hive -e "$ads_daas_user_total"
;;
ads_daas_user_retention )
	hive -e "$ads_daas_user_retention"
;;
ads_daas_pay_total )
	hive -e "$ads_daas_pay_total"
;;
ads_daas_order_total )
	hive -e "$ads_daas_order_total"
;;
ads_tob_company_total )
	hive -e "$ads_tob_company_total"
;;
ads_todesk_province_total )
	hive -e "$ads_todesk_province_total"
;;
ads_connect_active_total )
	hive -e "$ads_connect_active_total"
;;
ads_user_limit_total )
	hive -e "$ads_user_limit_total"
;;
ads_daas_dprovince_session_day )
	hive -e "$ads_daas_dprovince_session_day"
;;
ads_platform_detail_total )
	hive -e "$ads_platform_detail_total"
;;
ads_daas_forward_failed_total )
	hive -e "$ads_daas_forward_failed_total"
;;
ads_daas_client_session_day )
	hive -e "$ads_daas_client_session_day"
;;
ads_ec_total )
	hive -e "$ads_ec_total"
;;
ads_todesk_forward_failed_total )
	hive -e "$ads_todesk_forward_failed_total"
;;
ads_gaas_paying_user_total )
	hive -e "$ads_gaas_paying_user_total"
;;
ads_gaas_free_user_retention )
	hive -e "$ads_gaas_free_user_retention"
;;
ads_gaas_paying_user_retention )
	hive -e "$ads_gaas_paying_user_retention"
;;
ads_platform_file_detail_total )
	hive -e "$ads_platform_file_detail_total"
;;
ads_platform_vc_detail_total )	
	hive -e "$ads_platform_vc_detail_total"
;;
test )
	hive -e "$ads_daas_selling_time_total$ads_daas_user_total$ads_daas_user_retention$ads_daas_user_gold_total$ads_daas_newuser_total$ads_daas_user_distributed_total$ads_cloud_new_user_total$ads_cloud_paying_user_total$ads_cloud_connected_user_total"
;;
test1 )
	hive -e "$ads_file_transport_total$ads_daas_serv_session_day$ads_daas_country_session_day$ads_daas_sprovince_session_day$ads_daas_dprovince_session_day$ads_daas_handshake$ads_connect_limit_total$ads_daas_licode_handshake$ads_daas_selling_time_total$ads_daas_user_total$ads_daas_order_total$ads_tob_company_total$ads_connect_active_total$ads_user_limit_total$ads_daas_forward_failed_total$ads_daas_client_session_day"
;;
test2 )
	hive -e "$ads_ec_total$ads_todesk_forward_failed_total$ads_daas_user_gold_total$ads_daas_newuser_total$ads_daas_user_distributed_total$ads_daas_user_retention$ads_daas_isp_handshake$ads_daas_used_time_total$ads_platform_detail_total$ads_gaas_paying_user_total$ads_gaas_free_user_retention$ads_gaas_paying_user_retention$ads_platform_file_detail_total$ads_platform_vc_detail_total"
;;

    cc )
	hive -e "$ads_licode_connection_action_4500$ads_backend_ec_error_reason_all_4500$ads_backend_ec_error_reason_both_4500$ads_licode_connection_stream_4500$ads_backend_ea_error_reason_all_4500$ads_backend_ea_error_reason_both_4500$ads_serv_recover_total$ads_product_banner_total$ads_licode_sfu_total$ads_mobile_vedio_total$ads_mobile_conn_fst_time_total$ads_mobile_dec_total$ads_mobile_enc_total$ads_mobile_rtc_cap_rate$ads_mobile_rtc_tar_rate$ads_mobile_rtc_rtt$ads_mobile_rtc_loss$ads_su_country_total$ads_du_country_total$ads_bad_serv_top$ads_tob_bandwidth_total$ads_vip_conn_duration$ads_process_session_day"
;;
#删掉得$ads_mac_avg_total$ads_serv_all_not_present_total$dwd_anti_fraud_data$dwd_anti_fraud_order$ads_tob_bandwidth_total
    all_after )
	hive -e "$ads_serv_recover_total$ads_product_banner_total$ads_licode_sfu_total$ads_mobile_vedio_total$ads_mobile_conn_fst_time_total$ads_mobile_dec_total$ads_mobile_enc_total$ads_mobile_rtc_cap_rate$ads_mobile_rtc_tar_rate$ads_mobile_rtc_rtt$ads_mobile_rtc_loss$ads_su_country_total$ads_du_country_total$ads_bad_serv_top$ads_vip_conn_duration$ads_process_session_day$ads_client_activesu$ads_client_activedu$ads_file_transport_total$ads_daas_serv_session_day$ads_daas_country_session_day$ads_daas_sprovince_session_day$ads_daas_dprovince_session_day$ads_daas_handshake$ads_connect_limit_total$ads_daas_licode_handshake$ads_daas_selling_time_total$ads_daas_user_total$ads_daas_order_total$ads_tob_company_total$ads_connect_active_total$ads_user_limit_total$ads_daas_forward_failed_total$ads_daas_client_session_day$ads_ec_total$ads_todesk_forward_failed_total$ads_daas_user_gold_total$ads_daas_newuser_total$ads_daas_user_distributed_total$ads_daas_user_retention$ads_daas_isp_handshake$ads_daas_used_time_total$ads_gaas_paying_user_total$ads_gaas_free_user_retention$ads_gaas_paying_user_retention$ads_platform_file_detail_total$ads_platform_vc_detail_total"
;;
    all )
	hive -e "$ads_session_day$ads_conn_duration$ads_su_client_version_total$ads_du_client_version_total$ads_client_connect$ads_client_closed$ads_user_logout$ads_add_device$ads_delete_device$ads_edit_device$ads_add_group$ads_delete_group$ads_edit_group$ads_android_logic_total$ads_android_connect_total$ads_phone_new_user$ads_android_su_action$ads_android_bus$ads_android_resolution$ads_android_bios$ads_android_signin_total$ads_sclient_session$ads_phone_user_version$ads_bus_new_user$ads_bus_user_start$ads_new_user$ads_start_user$ads_client_new_mac$ads_client_start_mac$ads_client_new_su$ads_client_start_su$ads_client_new_du$ads_client_start_du$ads_bus_user_retention$ads_user_retention$ads_mac_retention$ads_su_retention$ads_du_retention$ads_version_mac_total$ads_su_handshake_total$ads_du_handshake_total$ads_su_handshake_failure$ads_du_handshake_failure$ads_user_login_total$ads_vendor_process_total$ads_windows_first_frame_total$ads_android_action_total$ads_android_action_pvuv$ads_android_du_total$ads_ip_user_login_total$ads_android_setting_total$ads_android_test_double$ads_client_version_retention$ads_forward_hour_isp_total$ads_ec_hour_isp_total$ads_ea_hour_isp_total$ads_windows_tfa_total$ads_sec_verif_total$ads_cross_provincial_total$ads_gaas_game_total$ads_dass_connect_fail_total$ads_limit_user_total$ads_limit_tpy_total"
    ;;
    all_1 )
	hive -e "$ads_client_version_new_su$ads_client_version_du_total$ads_client_version_du_retention$ads_rtc_version_conn_total$ads_rtc_version_vedio_total$ads_rtc_version_fallback_total$ads_rtc_version_conn_fst_time$ads_rtc_version_cap_send$ads_rtc_version_rcvd_rend$ads_rtc_version_du_rate$ads_rtc_version_rtt$ads_rtc_version_loss$ads_android_login_total$ads_web_setting_total$ads_su_province_total$ads_android_licode_total$ads_ios_licode_total$ads_licode_sdk_total$ads_licode_sig_total$ads_licode_ice_total$ads_windows_first_frame_fail$ads_windows_first_frame_success$ads_licode_conn_time_total$ads_version_new_connect_total$ads_version_new_connect_clientype_total$ads_backend_licode_loss_total$ads_windows_clarity_total$ads_id_request_phone_total$ads_version_new_connect_type_total$ads_android_logic_topic_total$ads_android_logic_retention$ads_p2p_network_total$ads_serv_diff_total$ads_client_newuser_newsu$ads_client_newuser_suretention$ads_client_newuser_newdu$ads_client_newuser_duretention$ads_bigdata_datas_total$ads_pc_new_version_total$ads_gaas_active_user_total$ads_gaas_active_mac_total$ads_gaas_active_user_retention$ads_cloud_paying_user_total$ads_cloud_new_user_total$ads_cloud_connected_user_total$ads_platform_detail_total"
;;
esac

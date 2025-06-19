#!/bin/bash

APP=todesk

if [ $# -eq 1 ] ;then
     n_date=`date -d "$1" +"%Y-%m-%d"`
else
     n_date=`date -d " -1 days" +"%Y-%m-%d"`
fi
do_date=`date -d "${n_date} -1 days" +"%Y-%m-%d"`
count=0

until [ $count -eq '5' ];do

do_date=`date -d "${do_date} +1 days" +"%Y-%m-%d"`
let count+=1
echo "t_date ${do_date}"
echo "count ${count}"

sql="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert into table ${APP}.ads_limit_user_total
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


##set spark.executor.memory=8g;
#                set spark.driver.memory=1g;
 #               set spark.executor.cores=8;
  #              set spark.executor.instances=3;

         all_sql=$all_sql$sql

done 
hive -e "
                set hive.support.quoted.identifiers=None;
                set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
	$all_sql	
"

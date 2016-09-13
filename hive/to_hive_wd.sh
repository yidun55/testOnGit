#!/bin/sh


yy='20150803'
su hdfs <<EOF
hdfs dfs -mkdir /data/wangdai/${yy}
hdfs dfs -put /home/hdfs/crawler/dyh_data/wangdai/wangdai_p2pzxw_2015_8_3 /data/wdheimingdan/${yy}/${yy}.txt;
EOF

echo '---------------´´½¨Íâ²¿±í---------------'
sudo -u hdfs hive <<EOF
use spss;
create external table if not exists spss.c_wd_blacklist_tmp(
vname string,
id_number string,
sex string,
identity_card_address string,
home_address string,
telephone string,
total_arrears string,
overdue_penalty string,
overdue_sum string,
web_behalf_sum string,
maximum_late_days string,
update_date string,
insert_time string
) partitioned by (ymd string);

alter table spss.c_wd_blacklist_tmp add if not exists partition(ymd='${yy}')
location "/data/wdheimingdan/${yy}";
EOF

echo '-----------------create table------------------'
sudo -u hdfs hive <<EOF
create table if not exists spss.c_wd_blacklist(
vname string,
id_number string,
sex string,
identity_card_address string,
home_address string,
telephone string,
total_arrears string,
overdue_penalty string,
overdue_sum string,
web_behalf_sum string,
maximum_late_days string,
update_date string,
insert_time string
) partitioned by (ymd string);
EOF

echo '---------------´´½¨¹ÜÀí±í---------------'
sudo -u hdfs hive <<EOF
insert overwrite table spss.c_wd_blacklist partition(ymd='${yy}')
select
regexp_replace(regexp_replace(vname,'\r',''),'\n','') as vname,
regexp_replace(regexp_replace(id_number,'\r',''),'\n','') as id_number,
regexp_replace(regexp_replace(sex,'\r',''),'\n','') as sex,
regexp_replace(regexp_replace(identity_card_address,'\r',''),'\n','') as identity_card_address,
regexp_replace(regexp_replace(home_address,'\r',''),'\n','') as home_address,
regexp_replace(regexp_replace(telephone,'\r',''),'\n','') as telephone,
regexp_replace(regexp_replace(total_arrears,'\r',''),'\n','') as total_arrears,
regexp_replace(regexp_replace(overdue_penalty,'\r',''),'\n','') as overdue_penalty,
regexp_replace(regexp_replace(overdue_sum,'\r',''),'\n','') as overdue_sum,
regexp_replace(regexp_replace(web_behalf_sum,'\r',''),'\n','') as web_behalf_sum,
regexp_replace(regexp_replace(maximum_late_days,'\r',''),'\n','') as maximum_late_days,
regexp_replace(regexp_replace(update_date,'\r',''),'\n','') as update_date,
regexp_replace(regexp_replace(insert_time,'\r',''),'\n','') as insert_time
from spss.c_wd_blacklist_tmp where ymd='${yy}' 
group by vname,id_number,sex,identity_card_address,
home_address,telephone,total_arrears,overdue_penalty,
overdue_sum,web_behalf_sum,maximum_late_days,update_date,insert_time;
EOF


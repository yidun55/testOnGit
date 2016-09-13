#!/bin/sh


#hdfs dfs -put /home/dyh/data/credit/person/personMore /data/personMore/personMore.txt;


echo '---------------´´½¨Íâ²¿±í---------------'
sudo -u hdfs hive <<EOF
use spss;
create external table if not exists spss.c_ppai_blacklist_tmp(
accumulated_principal string,
maximum_late_days string,
id string,
loan_period string,
loan_date string,
overdue_days string,
overdue_interest string,
province string,
user_name string,
vname string,
telephone string,
identity_card_number string
update_time string
)
row format delimited fields terminated by '\001'
location '/data/ppaiheimingdan/';
EOF

echo '---------------´´½¨¹ÜÀí±í---------------'
sudo -u hdfs hive <<EOF
use spss;
create table spss.c_ppai_blacklist as
select
regexp_replace(regexp_replace(accumulated_principal,'\r',''),'\n','') as accumulated_principal,
regexp_replace(regexp_replace(maximum_late_days,'\r',''),'\n','') as maximum_late_days,
regexp_replace(regexp_replace(id,'\r',''),'\n','') as id,
regexp_replace(regexp_replace(loan_period,'\r',''),'\n','') as loan_period,
regexp_replace(regexp_replace(overdue_days,'\r',''),'\n','') as overdue_days,
regexp_replace(regexp_replace(overdue_interest,'\r',''),'\n','') as overdue_interest,
regexp_replace(regexp_replace(province,'\r',''),'\n','') as province,
regexp_replace(regexp_replace(user_name,'\r',''),'\n','') as user_name,
regexp_replace(regexp_replace(vname,'\r',''),'\n','') as vname,
regexp_replace(regexp_replace(telephone,'\r',''),'\n','') as telephone,
regexp_replace(regexp_replace(identity_card_number,'\r',''),'\n','') as identity_card_number,
regexp_replace(regexp_replace(update_time,'\r',''),'\n','') as update_time

from spss.c_ppai_blacklist_tmp;
EOF
 
# echo '---------------µ¼ÈëÊý¾Ý---------------'
# sudo -u hdfs hive <<EOF
# insert overwrite table spss.c_personmore_increment
# select
# unbase64(id) int,
# unbase64(name) string,
# unbase64(caseCode) string,
# unbase64(age) int,
# unbase64(sexy) string,
# unbase64(CardNum) string,
# unbase64(courtName) string,
# unbase64(areaName) string,
# unbase64(partyTypeName) int,
# unbase64(gistId) string,
# unbase64(regDate) string,
# unbase64(gistUnit) string,
# unbase64(duty) double,
# unbase64(performance) string,
# unbase64(disruptTypeName) string,
# unbase64(pulishDate) string
# from spss.c_personmore_increment_tmp;
# EOF
 
# echo '---------------Éú³Éorcle±í---------------'
# sudo -u hdfs hive <<EOF
# create table spss.c_personmore_increment(
# id int,
# name varchar2(50),
# caseCode varchar2(500),
# age int,
# sexy varchar2(50),
# CardNum varchar2(50),
# courtName varchar2(500),
# areaName varchar2(50),
# partyTypeName int,
# gistId varchar2(500),
# regDate varchar2(50),
# gistUnit varchar2(500),
# duty double,
# performance varchar2(500),
# disruptTypeName varchar2,
# pulishDate string
# );
# EOF


#!/bin/sh

yy='20150803'
su hdfs <<EOF
hdfs dfs -mkdir /data/dailmheimingdan/${yy}
hdfs dfs -put /home/hdfs/crawler/dyh_data/dailianmeng/dailianmeng_blacklist_2015_8_3 /data/dailmheimingdan/${yy}/${yy}.txt;
EOF

echo '---------------´´½¨Íâ²¿±í---------------'
sudo -u hdfs hive <<EOF
use spss;
create external table if not exists spss.c_dailm_blacklist_tmp(
identity_card_number string,
sex string,
birthday string,
age string,
issued_address string,
telephone string,
email string,
qq string,
vname string,
principal_and_interest string,
already_amount string,
not_penalty string,
loan_date string,
loan_period string,
company string,
company_telephone string,
company_address string,
residential_telephone string,
residential_address string,
certificate_address string,
insert_time string
) partitioned by (ymd string);

alter table c_dailm_blacklist_tmp add if not exists partition(ymd='${yy}')
location "/data/dailmheimingdan/${yy}";
EOF

echo '-----------------create table------------------'
sudo -u hdfs hive <<EOF
create table if not exists spss.c_dailm_blacklist(
identity_card_number string,
sex string,
birthday string,
age string,
issued_address string,
telephone string,
email string,
qq string,
vname string,
principal_and_interest string,
already_amount string,
not_penalty string,
loan_date string,
loan_period string,
company string,
company_telephone string,
company_address string,
residential_telephone string,
residential_address string,
certificate_address string,
insert_time string
) partitioned by (ymd string);
EOF

echo '---------------´´½¨¹ÜÀí±í---------------'
sudo -u hdfs hive <<EOF
insert overwrite table spss.c_dailm_blacklist partition(ymd='${yy}')
select
regexp_replace(regexp_replace(identity_card_number,'\r',''),'\n','') as identity_card_number,
regexp_replace(regexp_replace(sex,'\r',''),'\n','') as sex,
regexp_replace(regexp_replace(birthday,'\r',''),'\n','') as birthday,
regexp_replace(regexp_replace(age,'\r',''),'\n','') as age,
regexp_replace(regexp_replace(issued_address,'\r',''),'\n','') as issued_address,
regexp_replace(regexp_replace(telephone,'\r',''),'\n','') as telephone,
regexp_replace(regexp_replace(email,'\r',''),'\n','') as email,
regexp_replace(regexp_replace(qq,'\r',''),'\n','') as qq,
regexp_replace(regexp_replace(vname,'\r',''),'\n','') as vname,
regexp_replace(regexp_replace(principal_and_interest,'\r',''),'\n','') as principal_and_interest,
regexp_replace(regexp_replace(already_amount,'\r',''),'\n','') as already_amount,
regexp_replace(regexp_replace(not_penalty,'\r',''),'\n','') as not_penalty,
regexp_replace(regexp_replace(loan_date,'\r',''),'\n','') as loan_date,
regexp_replace(regexp_replace(loan_period,'\r',''),'\n','') as loan_period,
regexp_replace(regexp_replace(company,'\r',''),'\n','') as company,
regexp_replace(regexp_replace(company_telephone,'\r',''),'\n','') as company_telephone,
regexp_replace(regexp_replace(company_address,'\r',''),'\n','') as company_address,
regexp_replace(regexp_replace(residential_telephone,'\r',''),'\n','') as residential_telephone,
regexp_replace(regexp_replace(residential_address,'\r',''),'\n','') as residential_address,
regexp_replace(regexp_replace(certificate_address,'\r',''),'\n','') as certificate_address,
regexp_replace(regexp_replace(insert_time,'\r',''),'\n','') as insert_time
from spss.c_dailm_blacklist_tmp where ymd='${yy}' group by identity_card_number,sex,birthday,age,
issued_address,telephone,email,qq,vname,principal_and_interest,
already_amount,not_penalty,loan_date,loan_period,company,company_telephone,
company_address,residential_telephone,residential_address,
certificate_address,insert_time;
EOF

#!/bin/sh

yy='201506'
su hdfs <<EOF
hdfs dfs -mkdir /data/zhuanli/${yy}
hdfs dfs -put /home/hdfs/crawler/dyh_data/zhuanli/${yy} /data/zhuanli/${yy}/${yy}.txt;
EOF

echo '---------------´´½¨Íâ²¿±í---------------'
sudo -u hdfs hive <<EOF
use spss;
create external table if not exists spss.c_patent_tmp(
class_no string,
address string,
application_date string,
patent_name string,
release_date string,
summary string,
release_no string,
agent_name string,
file_name string,
application_no string,
applicant_name string,
patent_agency string,
inventor string
) partitioned by (ymd string);

alter table c_patent_tmp add if not exists partition(ymd='${yy}')
location "/data/zhuanli/${yy}";
EOF

echo '-----------------create table------------------'
sudo -u hdfs hive <<EOF
create table if not exists spss.c_patent(
class_no string,
address string,
application_date string,
patent_name string,
release_date string,
summary string,
release_no string,
agent_name string,
file_name string,
application_no string,
applicant_name string,
patent_agency string,
inventor string
) partitioned by (ymd string);
EOF

echo '---------------´´½¨¹ÜÀí±í---------------'
sudo -u hdfs hive <<EOF
insert overwrite table spss.c_patent partition(ymd='${yy}')
select 
class_no,
address,
application_date,
patent_name,
release_date,
regexp_replace(summary,'\\s*',''),
release_no,
agent_name,
application_no,
applicant_name,
patent_agency,
inventor
from spss.c_patent_tmp where ymd='${yy}'
group by class_no,address,application_date,patent_name,release_date,
regexp_replace(summary,'\\s*',''),release_no,agent_name,application_no,applicant_name,patent_agency,inventor;
EOF
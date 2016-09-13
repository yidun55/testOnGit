#!/bin/sh

su hdfs <<EOF
hdfs dfs -mkdir /data/specialworker/phamacist
hdfs dfs -put /home/hdfs/crawler/dyh_data/specialworker/phamacist/phamacist.txt /data/specialworker/phamacist;
EOF

echo '---------------create external table---------------'
sudo -u hdfs hive <<EOF
create external table if not exists spss.c_pharmacist_tmp(
name string comment "姓名",
regID string comment "注册证号",
GZDW string comment "工作单位",
status string comment "状态",
ZCZYXQ string comment "注册证有效期"
)
row format delimited fields terminated by '\001'
location '/data/specialworker/phamacist';
EOF

echo '-----------------create table------------------'
sudo -u hdfs hive <<EOF
create table if not exists spss.c_pharmacist(
name string comment "姓名",
regID string comment "注册证号",
GZDW string comment "工作单位",
status string comment "状态",
ZCZYXQ string comment "注册证有效期"
)
comment "从浙江省食品监督管理局抓取执业药师注册证http://fw.zjfda.gov.cn/sp/personnel!new_app_zyysList.do"
row format delimited fields terminated by '\001'
tblproperties("creator"='dyh', 'created_at'="2014/10/15"); 
EOF

echo '---------------insert in table from external table---------------'
sudo -u hdfs hive <<EOF
insert overwrite table spss.c_pharmacist 
select
name,
regID,
GZDW,
status,
ZCZYXQ
from spss.c_pharmacist_tmp
group by regID;
EOF
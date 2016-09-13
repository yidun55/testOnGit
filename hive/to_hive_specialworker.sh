#!/bin/sh

su hdfs <<EOF
hdfs dfs -mkdir /data/specialworker
hdfs dfs -put /home/hdfs/crawler/dyh_data/specialworker/special.txt /data/specialworker/;
EOF

echo '---------------create external table---------------'
sudo -u hdfs hive <<EOF
create external table if not exists spss.c_specialworker_tmp(
name string comment "姓名",
ZYLB string comment "作业类别",
CZXM string comment "操作项目",
GZDW string comment "工作单位",
ZJSBH string comment "证件识别号",
sex string comment "性别",
identity_card_number string comment "身份证号"
)
row format delimited fields terminated by '\001'
location '/data/specialworker/';
EOF

echo '-----------------create table------------------'
sudo -u hdfs hive <<EOF
create table if not exists spss.c_specialworker(
name string comment "姓名",
ZYLB string comment "作业类别",
CZXM string comment "操作项目",
GZDW string comment "工作单位",
ZJSBH string comment "证件识别号",
sex string comment "性别",
identity_card_number string comment "身份证号"
) row format delimited fields terminated by '\001';
EOF


echo '---------------insert in table from external table---------------'
sudo -u hdfs hive <<EOF
insert overwrite table spss.c_specialworker 
select
name,
ZYLB ,
CZXM ,
GZDW ,
ZJSBH ,
sex,
identity_card_number
from spss.c_specialworker_tmp;
EOF
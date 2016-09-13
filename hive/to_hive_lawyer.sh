#!/bin/sh

su hdfs <<EOF
hdfs dfs -mkdir /data/specialworker/lawyer_info
hdfs dfs -put /home/hdfs/crawler/dyh_data/specialworker/lawyer_info/lawyer_info.txt /data/specialworker/lawyer_info;
EOF


echo '---------------create external table---------------'
sudo -u hdfs hive <<EOF
create external table if not exists spss.c_lawyer_info_tmp(
ZYZH string comment "执业证号",
name string comment "姓名",
RYLX string comment "人员类型",
sexy string comment "性别",
ZYJG string comment "执业机构"
)
row format delimited fields terminated by '\001'
location '/data/specialworker/lawyer_info';
EOF


echo '-----------------create table------------------'
sudo -u hdfs hive <<EOF
create table if not exists spss.c_lawyer_info(
ZYZH string comment "执业证号"
name string comment "姓名",
RYLX string comment "人员类型",
sexy string comment "性别",
ZYJG string comment "执业机构"
)
comment "从浙江政务服务网下载的律师个人信息"
row format delimited fields terminated by '\001'
tblproperties("creator"='dyh', 'created_at'="2014/10/15");
EOF

echo '---------------insert in table from external table---------------'
sudo -u hdfs hive <<EOF
insert overwrite table spss.c_lawyer_info 
select
ZYZH,
name,
RYLX,
sexy,
ZYJG
from spss.c_lawyer_info_tmp
group by ZYZH,name,RYLX,sexy,ZYJG;
EOF
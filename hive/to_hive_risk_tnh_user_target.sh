#!/bin/sh


yy='20151214'
su hdfs <<EOF
hdfs dfs -mkdir /data/mllib/jinrong/${yy}
hdfs dfs -put /home/hdfs/data/jinrong/risk_tnh_user_target_clean.txt /data/mllib/jinrong/${yy}/${yy}.txt;
EOF



echo '---------------creat external table---------------'
sudo -u hdfs hive <<EOF
use graphx;
create external table if not exists graphx.risk_tnh_user_target_tmp(
MOBILE_NUM string comment '手机号',
FLAG string comment "是否违约"
) partitioned by (ymd string);

alter table risk_tnh_user_target_tmp add if not exists partition(ymd='${yy}')
location "/data/mllib/jinrong/${yy}";
EOF


echo '-----------------create table------------------'
sudo -u hdfs hive <<EOF
create table if not exists graphx.risk_tnh_user_target(
MOBILE_NUM string comment '手机号',
FLAG string comment "是否违约"
) partitioned by (ymd string);
EOF


echo '---------------insert data---------------'
sudo -u hdfs hive <<EOF
insert overwrite table graphx.risk_tnh_user_target partition(ymd='${yy}')
select
regexp_replace(regexp_replace(MOBILE_NUM,'\r',''),'\n','') as MOBILE_NUM,
regexp_replace(regexp_replace(FLAG,'\r',''),'\n','') as FLAG
from graphx.risk_tnh_user_target_tmp
EOF

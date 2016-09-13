#!/bin/sh


yy='20151223'
su hdfs <<EOF
hdfs dfs -mkdir /data/mllib/jinrong/${yy}
hdfs dfs -put /home/hdfs/data/jinrong/risk/risk_tnh_user_target_test.txt /data/mllib/jinrong/${yy}/${yy}.txt;
EOF



echo '---------------creat external table---------------'
sudo -u hdfs hive <<EOF
use graphx;
create external table if not exists graphx.risk_tnh_user_target_test_tmp(
MOBILE_NUM string comment '手机号'
) partitioned by (ymd string);

alter table risk_tnh_user_target_test_tmp add if not exists partition(ymd='${yy}')
location "/data/mllib/jinrong/${yy}";
EOF


echo '-----------------create table------------------'
sudo -u hdfs hive <<EOF
create table if not exists graphx.risk_tnh_user_target_test(
MOBILE_NUM string comment '手机号'
) partitioned by (ymd string);
EOF


echo '---------------insert data---------------'
sudo -u hdfs hive <<EOF
insert overwrite table graphx.risk_tnh_user_target_test partition(ymd='${yy}')
select
regexp_replace(regexp_replace(MOBILE_NUM,'\r',''),'\n','') as MOBILE_NUM
from graphx.risk_tnh_user_target_test_tmp
EOF

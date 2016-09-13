#!/bin/sh

# """
# unitMore说明文件
# """
# 1，用"\001"分割
# 2，字段自左向右分别是：id号，被执行人姓名/名称，案号，法定代表人或者负责人姓名，身份证号码/组织机构代码，执行法院，省份，执行依据文号，立案时间，做出执行依据单位，生效法律文书确定的义务，被执行人的履行情况，失信被执行人行为具体情形，发布时间。
#   注：第个字段分别用base64加密
#       test_base64.py是一个测试解密的python脚本

# dfs -mkdir /data/xinyong;
# dfs -put /home/dyh/data/credit/unit/uniteMore  /data/xinyong/xinyong.txt;

# --创建外部表存放原始信用爬虫数据

yy='20150618'
su hdfs <<EOF
hdfs dfs -mkdir /data/xinyong/${yy}
hdfs dfs -put /home/hdfs/crawler/dyh_data/unit/uniteMore /data/xinyong/${yy}/${yy}.txt;
EOF

echo '---------------´´½¨Íâ²¿±í---------------'
sudo -u hdfs hive <<EOF
use spss;
create external table if not exists spss.c_unitmore_tmp(
cid string,
name string,
casecode string,
cardnum string,
businessentity string,
courtname string,
areaname string,
gistid string,
regdate string,
gistunit string,
duty string,
performance string,
disrupttypename string,
pulishdate string
) partitioned by (ymd string);

alter table c_unitmore_tmp add if not exists partition(ymd='${yy}')
location "/data/xinyong/${yy}";
EOF

echo '-----------------create table------------------'
sudo -u hdfs hive <<EOF
create table if not exists spss.c_unitmore(
cid string,
name string,
casecode string,
cardnum string,
businessentity string,
courtname string,
areaname string,
gistid string,
regdate string,
gistunit string,
duty string,
performance string,
disrupttypename string,
pulishdate string
) partitioned by (ymd string);
EOF

echo '---------------´´½¨¹ÜÀí±í---------------'
sudo -u hdfs hive <<EOF
insert overwrite table spss.spss.c_unitmore partition(ymd='${yy}')
select
regexp_replace(regexp_replace(cast(unbase64(cid) as string),'\r',''),'\n','') as id,
regexp_replace(regexp_replace(cast(unbase64(cid) as string),'\r',''),'\n','') as cid,
regexp_replace(regexp_replace(cast(unbase64(name) as string),'\r',''),'\n','') as name,
regexp_replace(regexp_replace(cast(unbase64(casecode) as string),'\r',''),'\n','') as casecode,
regexp_replace(regexp_replace(cast(unbase64(businessentity) as string),'\r',''),'\n','') as businessentity,
regexp_replace(regexp_replace(cast(unbase64(cardnum) as string),'\r',''),'\n','') as cardnum,
regexp_replace(regexp_replace(cast(unbase64(courtname) as string),'\r',''),'\n','') as courtname,
regexp_replace(regexp_replace(cast(unbase64(areaname) as string),'\r',''),'\n','') as areaname,
regexp_replace(regexp_replace(cast(unbase64(gistid) as string),'\r',''),'\n','') as gistid,
regexp_replace(regexp_replace(cast(unbase64(regdate) as string),'\r',''),'\n','') as regdate,
regexp_replace(regexp_replace(cast(unbase64(gistunit) as string),'\r',''),'\n','') as gistunit,
regexp_replace(regexp_replace(cast(unbase64(duty) as string),'\r',''),'\n','') as duty,
regexp_replace(regexp_replace(cast(unbase64(performance) as string),'\r',''),'\n','') as performance,
regexp_replace(regexp_replace(cast(unbase64(disrupttypename) as string),'\r',''),'\n','') as disrupttypename,
regexp_replace(regexp_replace(cast(unbase64(pulishdate) as string),'\r',''),'\n','') as pulishdate,
cast(unix_timestamp() as int) as createtime
from spss.c_unitmore_tmp where ymd='${yy}' group by cid,
name,
casecode,
cardnum,
businessentity,
courtname,
areaname,
gistid,
regdate,
gistunit,
duty,
performance,
disrupttypename,
pulishdate;
EOF


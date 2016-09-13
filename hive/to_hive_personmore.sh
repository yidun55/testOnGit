#!/bin/sh


yy='20150618'
su hdfs <<EOF
hdfs dfs -mkdir /data/personMore/${yy}
hdfs dfs -put /home/hdfs/crawler/dyh_data/personMore/personMore /data/personMore/${yy}/${yy}.txt;
EOF



echo '---------------creat external table---------------'
sudo -u hdfs hive <<EOF
use spss;
create external table if not exists spss.c_personmore_tmp(
id int,
name string,
caseCode string,
age string,
sexy string,
CardNum string,
courtName string,
areaName string,
partyTypeName int,
gistId string,
regDate string,
gistUnit string,
duty string,
performance string,
disruptTypeName string,
pulishDate string
) partitioned by (ymd string);

alter table c_personmore_tmp add if not exists partition(ymd='${yy}')
location "/data/personMore/${yy}";
EOF


echo '-----------------create table------------------'
sudo -u hdfs hive <<EOF
create table if not exists spss.c_personmore(
id int,
cid int,
name string,
caseCode string,
age string,
sex string,
CardNum string,
courtName string,
areaName string,
gistId string,
regDate string,
gistUnit string,
duty string,
performance string,
disruptTypeName string,
pulishDate string,
createtime int,
partyTypeName int
) partitioned by (ymd string);
EOF


echo '---------------insert data---------------'
sudo -u hdfs hive <<EOF
insert overwrite table spss.c_personmore partition(ymd='${yy}')
select
regexp_replace(regexp_replace(cast(unbase64(id) as int),'\r',''),'\n','') as id,
regexp_replace(regexp_replace(cast(unbase64(id) as int),'\r',''),'\n','') as cid,
regexp_replace(regexp_replace(cast(unbase64(name) as string),'\r',''),'\n','') as name,
regexp_replace(regexp_replace(cast(unbase64(caseCode) as string),'\r',''),'\n','') as caseCode,
regexp_replace(regexp_replace(cast(unbase64(age) as string),'\r',''),'\n','') as age,
regexp_replace(regexp_replace(cast(unbase64(sexy) as string),'\r',''),'\n','') as sex,
regexp_replace(regexp_replace(cast(unbase64(CardNum) as string),'\r',''),'\n','') as CardNum,
regexp_replace(regexp_replace(cast(unbase64(courtName) as string),'\r',''),'\n','') as courtName,
regexp_replace(regexp_replace(cast(unbase64(areaName) as string),'\r',''),'\n','') as areaName,
regexp_replace(regexp_replace(cast(unbase64(gistId) as string),'\r',''),'\n','') as gistId,
regexp_replace(regexp_replace(cast(unbase64(regDate) as string),'\r',''),'\n','') as regDate,
regexp_replace(regexp_replace(cast(unbase64(gistUnit) as string),'\r',''),'\n','') as gistUnit,
regexp_replace(regexp_replace(cast(unbase64(duty) as string),'\r',''),'\n','') as duty,
regexp_replace(regexp_replace(cast(unbase64(performance) as string),'\r',''),'\n','') as performance,
regexp_replace(regexp_replace(cast(unbase64(disruptTypeName) as string),'\r',''),'\n','') as disruptTypeName,
regexp_replace(regexp_replace(cast(unbase64(pulishDate) as string),'\r',''),'\n','') as pulishDate,
cast(unix_timestamp() as int) as createtime,
regexp_replace(regexp_replace(cast(unbase64(partyTypeName) as int),'\r',''),'\n','') as partyTypeName
from spss.spss.c_personmore_tmp where ymd='${yy}' group by id,
name,
caseCode,
age,
sexy,
CardNum,
courtName,
areaName,
partyTypeName,
gistId,
regDate,
gistUnit,
duty,
performance,
disruptTypeName,
pulishDate;
EOF


sudo -u hdfs hive <<EOF
--regexp_replace(regexp_replace(if(length(cast(unbase64(duty) as string))<2000, cast(unbase64(duty) as string), substr(cast(unbase64(duty) as string),0,2000)),'\r',''),'\n','') as duty,
--regexp_replace(regexp_replace(if(length(cast(unbase64(disruptTypeName) as string))<2000, cast(unbase64(disruptTypeName) as string), substr(cast(unbase64(disruptTypeName) as string),0,2000)),'\r',''),'\n','') as disruptTypeName,
--unix_timestamp(regexp_replace(regexp_replace(cast(unbase64(pulishDate) as string),'\r',''),'\n',''),"y年m月d日") as createtime,
EOF
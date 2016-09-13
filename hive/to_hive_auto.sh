#!/bin/sh


yy='20150824'
su hdfs <<EOF
hdfs dfs -mkdir /data/auto/${yy}
hdfs dfs -put /home/hdfs/crawler/dyh_data/auto/final_allcountry.txt /data/auto/${yy}/${yy}.txt;
EOF



echo '---------------creat external table---------------'
sudo -u hdfs hive <<EOF
use spss;
create external table if not exists spss.c_auto_tmp(
type_auto string comment '车型',
brand string comment "品牌",
level string comment "级别",
BSX string comment "变速箱",
CSJG string comment "车身结构",
ZWGS string comment "坐位个数",
PL string comment "排量",
RLXS string comment "燃料",
QDFS string comment "驱动方式",
PZ string comment "配置，包含10个字段用'|'分割1代表有，2代表可选配，3代表没有。自左向右分别是天窗，电动调节坐椅，ESP，GPS，定速巡航，倒车雷达，多功能方向盘，氙气大灯，真皮坐椅，全自动空调",
SCFS string comment "生产方式",
id string comment "汽车的id",
YCG string comment "汽车原产国"
) partitioned by (ymd string);

alter table c_auto_tmp add if not exists partition(ymd='${yy}')
location "/data/auto/${yy}";
EOF


echo '-----------------create table------------------'
sudo -u hdfs hive <<EOF
create table if not exists spss.c_auto(
type_auto string comment '车型',
brand string comment "品牌",
level string comment "级别",
BSX string comment "变速箱",
CSJG string comment "车身结构",
ZWGS string comment "坐位个数",
PL string comment "排量",
RLXS string comment "燃料",
QDFS string comment "驱动方式",
PZ string comment "配置，包含10个字段用'|'分割1代表有，2代表可选配，3代表没有。自左向右分别是天窗，电动调节坐椅，ESP，GPS，定速巡航，倒车雷达，多功能方向盘，氙气大灯，真皮坐椅，全自动空调",
SCFS string comment "生产方式",
id string comment "汽车的id",
YCG string comment "汽车原产国",
createtime int comment "插入时间"
) partitioned by (ymd string);
EOF


echo '---------------insert data---------------'
sudo -u hdfs hive <<EOF
insert overwrite table spss.c_auto partition(ymd='${yy}')
select
regexp_replace(regexp_replace(type_auto,'\r',''),'\n','') as type_auto,
regexp_replace(regexp_replace(brand,'\r',''),'\n','') as brand,
regexp_replace(regexp_replace(level,'\r',''),'\n','') as level,
regexp_replace(regexp_replace(BSX,'\r',''),'\n','') as BSX,
regexp_replace(regexp_replace(CSJG,'\r',''),'\n','') as CSJG,
regexp_replace(regexp_replace(ZWGS,'\r',''),'\n','') as ZWGS,
regexp_replace(regexp_replace(PL,'\r',''),'\n','') as PL,
regexp_replace(regexp_replace(RLXS,'\r',''),'\n','') as RLXS,
regexp_replace(regexp_replace(QDFS,'\r',''),'\n','') as QDFS,
regexp_replace(regexp_replace(PZ,'\r',''),'\n','') as PZ,
regexp_replace(regexp_replace(SCFS,'\r',''),'\n','') as SCFS
regexp_replace(regexp_replace(id,'\r',''),'\n','') as id
regexp_replace(regexp_replace(YCG,'\r',''),'\n','') as YCG
cast(unix_timestamp() as int) as createtime
from spss.spss.c_auto_tmp
EOF

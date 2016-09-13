#!/bin/sh

su hdfs <<EOF
hdfs dfs -mkdir /data/specialworker/foodBeverage
hdfs dfs -put /home/hdfs/crawler/dyh_data/specialworker/foodBeverage/foodBeverage.txt /data/specialworker/foodBeverage;
EOF

echo '---------------create external table---------------'
sudo -u hdfs hive <<EOF
create external table if not exists spss.c_foodBeverage_tmp(
entName string comment "企业名称",
regLocation string comment "注册地址",
FDDBR string comment "法定代表人",
classi string comment "类别",
remark string comment "备注",
XKZH string comment "许可证号",
begin string comment "许可证有效日期起",
end string comment "许可证有效日期止",
issueDate string comment "许可证有效日期",
FZJG string comment "发证机关"
)
row format delimited fields terminated by '\001'
location '/data/specialworker/foodBeverage';
EOF

echo '-----------------create table------------------'
sudo -u hdfs hive <<EOF
create table if not exists spss.c_foodBeverage(
entName string comment "企业名称",
regLocation string comment "注册地址",
FDDBR string comment "法定代表人",
classi string comment "类别",
remark string comment "备注",
XKZH string comment "许可证号",
begin string comment "许可证有效日期起",
end string comment "许可证有效日期止",
issueDate string comment "许可证有效日期",
FZJG string comment "发证机关"
)
comment "从浙江省食品监督管理局抓取执业药师注册证http:fw.zjfda.gov.cn/sp/cyfw!new_app_cyfwList.do"
tblproperties("creator"='dyh', 'created_at'="2014/10/15") 
row format delimited fields terminated by '\001';
EOF


echo '---------------insert in table from external table---------------'
sudo -u hdfs hive <<EOF
insert overwrite table spss.c_foodBeverage 
select
entName,
regLocation,
FDDBR,
classi,
remark,
XKZH,
begin,
end,
issueDate,
FZJG
from spss.c_pharmacist_tmp
group by XKZH;
EOF
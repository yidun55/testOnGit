#!/bin/sh


echo '---------------insert data---------------'
sudo -u hdfs hive <<EOF

create table if not exists spss.sq_positive_sample(
phone_num string
)

insert overwrite table spss.sq_positive_sample 
select oi_contact_phone from ods.crm_or_order_info t1 join 
 (select op_orderid from ods.crm_or_order_product where op_name like "%收款宝%") t2 on t1.oi_id=t2.op_orderid
EOF
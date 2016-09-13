#!/bin/sh


echo '---------------insert data---------------'
sudo -u hdfs hive <<EOF
insert overwrite table spss.c_personmore partition(ymd='20150618')
select
id,
cid,
name,
caseCode,
age,
sex,
CardNum,
courtName,
areaName,
gistId,
regDate,
gistUnit,
duty,
performance,
disruptTypeName,
pulishDate,
createtime,
partyTypeName
from spss.c_personmore4 where name not like "%  %";
EOF

select count(1) from (select count(1) d from DIANXIAO.PRED_SOURCE_ALL where ymd='20150610' group by mobile_num having count(1)=1);

select count(mobile_num) from (select mobile_num,count(1) as d from DIANXIAO.PRED_SOURCE_ALL where ymd='20150610' group by mobile_num having d  =1);


select oi_contact_phone from ods.crm_or_order_info t1 join 
 (select op_orderid from ods.crm_or_order_product where op_name like "%收款宝%") t2 on t1.oi_id=t2.op_orderid   #lily's

select mobile_num from (select * from DIANXIAO.PRED_SOURCE_ALL where ymd = "20150610") t1 join 
 spss.sq_positive_sample t2 on t1.mobile_num=t2.phone_num;

select * from (select * from DIANXIAO.PRED_SOURCE_ALL where ymd = "20150610") t1 join 
 spss.sq_positive_sample t2 on t1.mobile_num=t2.phone_num;

insert overwrite local directory 'home/PRED_SOURCE_ALL_positive' select * from (select * from DIANXIAO.PRED_SOURCE_ALL where ymd = "20150610") t1 join 
 spss.sq_positive_sample t2 on t1.mobile_num=t2.phone_num limit 400000;    #将正样本写入到本地

insert overwrite local directory 'home/PRED_SOURCE_ALL_negative'
select t1.* from 
(select * from DIANXIAO.PRED_SOURCE_ALL where ymd = "20150610") t1 
left join spss.sq_positive_sample t2 on t1.mobile_num=t2.phone_num 
where t2.phone_num is null limit 400000;     #将负样本写入到本地

insert overwrite local directory 'home/skb_test_0827' select * from DIANXIAO.PRED_SOURCE_ALL t1 join 
 dianxiao.skb_test_0827 t2 on t1.mobile_num=t2.mobile;    #将正样本写入到本地

insert overwrite local directory 'home/skb_test_0901' select * from DIANXIAO.PRED_SOURCE_ALL t1 join 
 dianxiao.skb_test_0901 t2 on t1.mobile_num=t2.mobile;    #将正样本写入到本地




create external table if not exists dianxiao.mobile_num_pred_RandomForest_positiveRatio_1_tmp(
mobile_num string
)
location '/data/mllib/pred';

create table if not exists dianxiao.mobile_num_pred_RandomForest_positiveRatio_1(
mobile_num string
);

insert overwrite table dianxiao.mobile_num_pred_RandomForest_positiveRatio_1
select
mobile_num
from dianxiao.mobile_num_pred_RandomForest_positiveRatio_1_tmp;

HiveContext

insert overwrite local directory '/home/hdfs/data/jinrong//jinrong_pred_source_all_data' 
  select * from (select * from DIANXIAO.PRED_SOURCE_ALL where ymd='20151213') t1 join 
  graphx.risk_tnh_user_target t2 on t1.mobile_num=t2.mobile_num;    #Lily风控

insert overwrite local directory '/home/hdfs/data/esTest'
  select * from hdm.f_fact_trans_success_new_details;



insert overwrite table uts.dyh_bigguy SELECT * FROM UTS.ulb_collect_all where pk_mobile in ('13601616757', '18601920673','13911719371', '13602692360', '13801380616','13801021969','13901154251','13911890501','13907640690','13801255252','18911402345')




insert overwrite table ulb_collect_sample_20160611  partition(ymd)
   select * from uts.ulb_all_dupd_m where ymd=20160611 limit 200000;


insert overwrite table ulb_collect_sample_20160611  partition(ymd)
   select * from uts.ulb_all_dupd_m where pk_mobile in ('13601616757', '18601920673','13911719371', '13602692360', '13801380616','13801021969','13901154251','13911890501','13907640690','13801255252','18911402345')



import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row
val raw_df = sqlContext.sql("select * from graphx.test")
val final_data = raw_df.rdd.map{row=>
    val sites = Array(6, 8, 12, 24)
    val d = sites.map(i=>row(i).toString)
    Row.fromSeq(d.toSeq)
}

val fieldNames = "trans_date,trans_name,cardno1,total_am"
val schemaArr = fieldNames.split(",").map(field => StructField(field, StringType, true))
val schema = StructType(schemaArr)
val final_df = sqlContext.createDataFrame(final_data, schema)

val config_df = sqlContext.sql("select * from graphx.user_id_mobile_num_4streaming")

# val finalDF = config_df.join(final_df, config_df("cardno")===final_df("cardno1"),"right").drop("cardno1")

val finalDF = final_df.join(config_df, config_df("cardno")===final_df("cardno1"),"left").drop("cardno1")



insert overwrite table yxt.pos_atmtxnjnl_u
select distinct
t2.user_id,
nvl(t3.mobilephone,t3.userid) as mobile_num,
t1.cardno,
t1.txn_data as trans_date,
t1.txn_nm as trans_name,
t1.trans_amt as total_am
from hds.nuc_ecusr t3,
     (select custid as user_id,acctno as cardno from hds.xsl_cust_acct_list where stat='1' and custid rlike '^[0-9]{10}$' and custid>='1000000021'
      union all 
      select cast(cast(b.user_id as bigint) as string) as user_id,a.loan_pan as cardno from hds.xd_c_loan_apply a,hds.xd_c_apply_user b where a.id=b.id and trim(a.status)='T'
      union all 
      select cast(cast(b.user_id as bigint) as string) as user_id,a.return_pan as cardno from hds.xd_c_loan_apply a,hds.xd_c_apply_user b where a.id=b.id and trim(a.status)='T'
     ) t2,
     hdw.lklpos_atmtxnjnl t1
where t1.cardno=t2.cardno and t2.user_id=t3.userseq
and t1.ymd>='20150601';


#生成配置表    结果: 1000000031(user_id)      18980000507(mobile_num)
insert overwrite table graphx.user_id_mobile_num_4streaming
select distinct
t2.cardno,
nvl(t3.mobilephone,t3.userid) as mobile_num
from hds.nuc_ecusr t3,
     (select custid as user_id,acctno as cardno from hds.xsl_cust_acct_list where stat='1' and custid rlike '^[0-9]{10}$' and custid>='1000000021'
      union all 
      select cast(cast(b.user_id as bigint) as string) as user_id,a.loan_pan as cardno from hds.xd_c_loan_apply a,hds.xd_c_apply_user b where a.id=b.id and trim(a.status)='T'
      union all 
      select cast(cast(b.user_id as bigint) as string) as user_id,a.return_pan as cardno from hds.xd_c_loan_apply a,hds.xd_c_apply_user b where a.id=b.id and trim(a.status)='T'
     ) t2
where t2.user_id=t3.userseq;

#create 一个table(两个字段,user_id, mobile_num)
create table if not exists graphx.user_id_mobile_num_4streaming(cardno string, mobile_num string)

insert overwrite table graphx.test partition(ymd)
select * from hdw.lklpos_atmtxnjnl where cardno in (03427900040004194,3033657310025905,3033659310029721,3037525310105348,3037525312285239,3037791843876181,3037855310240518,3037855840000523,3037857850206628,3037885310020529);


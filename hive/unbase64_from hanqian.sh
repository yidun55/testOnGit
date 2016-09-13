"""
unitMore说明文件
"""
1，用"\001"分割
2，字段自左向右分别是：id号，被执行人姓名/名称，案号，法定代表人或者负责人姓名，身份证号码/组织机构代码，执行法院，省份，执行依据文号，立案时间，做出执行依据单位，生效法律文书确定的义务，被执行人的履行情况，失信被执行人行为具体情形，发布时间。
  注：第个字段分别用base64加密
      test_base64.py是一个测试解密的python脚本

dfs -mkdir /data/xinyong;
dfs -put /home/dyh/data/credit/unit/uniteMore  /data/xinyong/xinyong.txt;

--创建外部表存放原始信用爬虫数据
create external table if not exists spss.c_credit_tmp(
id string,
be_executed_name string,
case_number string,
legal_representative string,
id_number string,
executive_court string,
province string,
execute_basis_symbol string,
filing_date string,
execute_basis_unit string,
document_determine_obligate string,
be_executed_performance string,
dishonesty_be_executed_case string,
release_date string
)
row format delimited fields terminated by '\001'
location '/data/xinyong/';

create table spss.c_credit as
select
regexp_replace(regexp_replace(cast(unbase64(id) as string),'\r',''),'\n','') as id,
regexp_replace(regexp_replace(cast(unbase64(be_executed_name) as string),'\r',''),'\n','') as be_executed_name,
regexp_replace(regexp_replace(cast(unbase64(case_number) as string),'\r',''),'\n','') as case_number,
regexp_replace(regexp_replace(cast(unbase64(legal_representative) as string),'\r',''),'\n','') as legal_representative,
regexp_replace(regexp_replace(cast(unbase64(id_number) as string),'\r',''),'\n','') as id_number,
regexp_replace(regexp_replace(cast(unbase64(executive_court) as string),'\r',''),'\n','') as executive_court,
regexp_replace(regexp_replace(cast(unbase64(province) as string),'\r',''),'\n','') as province,
regexp_replace(regexp_replace(cast(unbase64(execute_basis_symbol) as string),'\r',''),'\n','') as execute_basis_symbol,
regexp_replace(regexp_replace(cast(unbase64(filing_date) as string),'\r',''),'\n','') as filing_date,
regexp_replace(regexp_replace(cast(unbase64(execute_basis_unit) as string),'\r',''),'\n','') as execute_basis_unit,
regexp_replace(regexp_replace(cast(unbase64(document_determine_obligate) as string),'\r',''),'\n','') as document_determine_obligate,
regexp_replace(regexp_replace(cast(unbase64(be_executed_performance) as string),'\r',''),'\n','') as be_executed_performance,
regexp_replace(regexp_replace(cast(unbase64(dishonesty_be_executed_case) as string),'\r',''),'\n','') as dishonesty_be_executed_case,
regexp_replace(regexp_replace(cast(unbase64(release_date) as string),'\r',''),'\n','') as release_date
from spss.c_credit_tmp;

select * from spss.c_credit 
group by 
id,
be_executed_name,
case_number,
legal_representative,
id_number,
executive_court,
province,
execute_basis_symbol,
filing_date,
execute_basis_unit,
document_determine_obligate,
be_executed_performance,
dishonesty_be_executed_case,
release_date
having count(1)>1;


create table spss.c_credit as
select
regexp_replace(cast(unbase64(id) as string),'\r','') as id,
cast(unbase64(be_executed_name) as string) as be_executed_name,
cast(unbase64(case_number) as string) as case_number,
cast(unbase64(legal_representative) as string) as legal_representative,
cast(unbase64(id_number) as string) as id_number,
cast(unbase64(executive_court) as string) as executive_court,
cast(unbase64(province) as string) as province,
cast(unbase64(execute_basis_symbol) as string) as execute_basis_symbol,
cast(unbase64(filing_date) as string) as filing_date,
cast(unbase64(execute_basis_unit) as string) as execute_basis_unit,
cast(unbase64(document_determine_obligate) as string) as document_determine_obligate,
cast(unbase64(be_executed_performance) as string) as be_executed_performance,
cast(unbase64(dishonesty_be_executed_case) as string) as dishonesty_be_executed_case,
cast(unbase64(release_date) as string) as release_date
from spss.c_credit_tmp;



select count(1) from spss.c_credit
group by
id,
be_executed_name,
case_number,
legal_representative,
id_number,
executive_court,
province,
execute_basis_symbol,
filing_date,
execute_basis_unit,
document_determine_obligate,
be_executed_performance,
dishonesty_be_executed_case,
release_date
having count(1)>1;


create table spss.c_credit(
id varchar2(50),
be_executed_name varchar2(200),
case_number varchar2(200),
legal_representative varchar2(200),
id_number varchar2(50),
executive_court varchar2(100),
province varchar2(50),
execute_basis_symbol varchar2(200),
filing_date varchar2(50),
execute_basis_unit varchar2(200),
document_determine_obligate varchar2(500),
be_executed_performance varchar2(2000),
dishonesty_be_executed_case varchar2(2000),
release_date varchar2(50)
)
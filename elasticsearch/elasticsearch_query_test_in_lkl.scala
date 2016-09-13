

import org.elasticsearch.spark.sql._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
val data = sqlContext.sql("SELECT * FROM UTS.ulb_collect_all")
data.saveToEs(Map(ES_RESOURCE_WRITE->"label_1/ulb_collect_all",ES_NODES->"10.1.60.132",
  ES_MAPPING_ID->"pk_mobile"))

val tedata = data.where("pk_mobile in ('13801380616','13801021969','13901154251','13911890501','13907640690','13602692360','13601616757','13911719371','13801255252','18911402345','13717518671','15810628105','15210637902','13146055559','13810067988','18911186260')").rdd


uts.ulb_collect_all_sample


{"filter":{
  "and" : [
    {
      "bool" : {
        "must" : [
          { "term" : {"_id":"15683067501"} },
          { "term" : {"as_name_cn":"王权"} }
        ]
      }
    }
    ]
  }
}


{
    "query":{
         "bool": {
              "must":[              
                { "term" : {"_id":"15683067501"} },
                { "term" : {"as_name_cn":"王权"} }
                ]
                     }

    }
}

{
    "query":{
        "bool":{
            "should":[              
                 {
                     "bool": {
                          "must":[              
                {
                   
                    "term":{"agent_name":"余额"}
                   
                },
                {
                    "term":{"agent_name":"查询"}
                }
                ]
                     }
                 }
                ],
                 "should":[              
                 {
                     "bool": {
                          "must":[              
                {
                   
                    "term":{"trans_name":"余额"}
                   
                },
                {
                    "term":{"trans_name":"查询"}
                }
                ]
                     }
                 }
                ]
        }
    }
}


18601106728
{
  "query":
        {
          "bool" : {
            "must" : [
             { "term" : {"_id":"18601106728"} },
             {
                "query_string" : {
                    "default_field" : "as_area_hukou_prov",
                    "query" : "湖南 OR 北京"
                 }
             }              

              ]
          }
        }
}


{
    "query": {
        "wildcard": {
            "pk_mobile": "1*" 
        }
    }
}

//查询null值
{"query":{"regexp":{"an_age":"n.*"}}}


{
    "query":
        {
            "bool" : {
                 "must" : [
                { "term" : {"an_gender":"男"} }, 
                {
        "wildcard": {
            "pk_mobile": "188232*" 
        }
    }
           
                ]
          }
        }
}

{"query":{"match":{"_id":{"query":keywords,"type":"phrase"}}}}


{"query":{"wildcard": {"pk_mobile": "188232*"}}}





//=========================2016/3/8=============================
{"query":{"bool":{"must":[
{"range":{"blr_yecx_amt_cmax_6m":{"from":"0", "to":"632260000"}}},
{"range":{"blr_yecx_dc_cnt_24m":{"from":"0", "to":"63"}}},
{"range":{"blf_qb_bc_date_fst":{"gt":"2012-06-31"}}},
{"range":{"blf_qb_bc_date_lst":{"gt":"2012-06-31"}}},
{"range":{"blf_qb_bc_date_lst":{"lt":"2016-02-28"}}},
{"query_string" : {"default_field" : "as_area_hukou_prov","query" : "湖南 OR 北京 OR 广东 OR 山西 OR 上海"}},
{"query_string" : {"default_field" : "as_area_live","query" : "北京 OR 广东 OR 上海"}},
{"query_string" : {"default_field" : "as_mail","query" : "163.com"}},
{"term":{"an_gender":"男"}},
{"term":{"an_zodiac":"龙"}},
{"term":{"bh_blp_ltrm_cmax":"手刷"}},
{"term":{"bh_blp_pt_cmax":"晚上"}},
{"term":{"as_marital":"已婚"}}]}
}
}






{"term":{"blm_prov_cmax_6m":"江苏"}},
{"term":{"blm_prov_cmax_9m":"江苏"}},







{"query":{"bool":{"must":[
{"range":{"blr_yecx_amt_cmax_6m":{"from":"0", "to":"632260000"}}},
{"range":{"blr_yecx_dc_cnt_24m":{"from":"0", "to":"63"}}},
{"range":{"blf_qb_bc_date_fst":{"gt":"2012-06-31"}}},
{"range":{"blf_qb_bc_date_lst":{"gt":"2012-06-31"}}},
{"range":{"blf_qb_bc_date_lst":{"lt":"2016-02-28"}}},
{"query_string" : {"default_field" : "as_area_hukou_prov","query" : "湖南 OR 北京 OR 广东 OR 山西 OR 上海"}},
{"query_string" : {"default_field" : "as_area_live","query" : "北京 OR 广东 OR 上海"}},
{"query_string" : {"default_field" : "as_mail","query" : "163.com"}},
{"term":{"an_gender":"男"}},
{"term":{"an_zodiac":"龙"}},
{"term":{"blm_prov_cmax_6m":"江苏"}},
{"term":{"blm_prov_cmax_9m":"江苏"}},
{"term":{"as_marital":"已婚"}}]}
}
}


{"query":{"bool":{"must":[
{ "term" : {"pk_mobile":"18570639692"}}
]}
}
}









//===============================test==================================
{"query":{"bool":{"must":[
{"query_string" : {"default_field" : "as_area_hukou_prov","query" : "湖南 OR 北京 OR 广东 OR 山西 OR 上海"}}]}
}
}




//====================测试用例===============================
{"query":{"bool":{"must":[
{"query_string" : {"default_field" : "bh_blp_pt_lst","query" : "晚上 OR 凌晨"}},
{"query_string" : {"default_field" : "blr_prov_lst","query" : "宁夏回族自治区"}},
{"query_string" : {"default_field" : "blr_prov_cmax_all","query" : "贵州 OR 黑龙江 OR 河南"}}
]}}}



{"query":{"bool":{"must":[
{"range":{"blf_qb_bc_date_fst":{"from":"2012-06-31", "to":"2015-06-31"}}}]}}}



{"query":{"bool":{"must":[
{"range":{"blr_yecx_amt_cmax_6m":{"from":"0", "to":"632260000"}}},
{"range":{"blr_yecx_dc_cnt_24m":{"from":"0", "to":"63"}}},
{"range":{"blf_qb_bc_date_fst":{"gt":"2012-06-31"}}},
{"range":{"blf_qb_bc_date_lst":{"gt":"2012-06-31"}}}]}
}
}


{
  "aggs":{
    "uniq_streets":{
      "cardinality":{
        "field":"an_gender"
      }
    }
  },
  "_source": ["pk_mobile", "an_gender"]
}


{"query":{"bool":{"must":[
{ "term" : {"pk_mobile":"13717518671"}}
]}
}
  "_source": ["as_edu_phd_major"]
}


{"query": {"bool": {"must": [{"query_string": {"query": "\"\u5929\u874e\" OR \"\u9b54\u874e\"",
   "default_field": "an_constel"}}]}}}  //query_string多条件查询时出错


curl -XPUT http://localhost:9200/test/trans/_mapping -d'
{
  "test": {
    "mappings": {
      "trans": {
        "properties": {
          "mobile_num": {
            "type": "string",
            "null_value": "NA"
          },
          "mobile_code": {
            "type": "string",
            "null_value": "NA"
          }
        }
      }
    }
  }
}'



curl -XPUT http://localhost:9200/test/trans/1 -d '{"mobile_num":"132", "mobile_code": null}'

{"query":{"match":{"mobile_code":"NA"}}}
{"query":{"match":{"mobile_num":"132"}}}
curl -XPOST http://localhost:9200/test/trans/_search -d '{"query":{"match":{"mobile_num":"132"}}}'


{"query":{"bool":{"must":[
{"range":{"an_age":{"from":"1", "to":"99"}}}]}}}

{"query":{"bool":{"must":[{"term":{"an_gender":"NA"}}]}}}
{"query":{"match":{"an_gender":"NA"}}}

{"query":{"bool":{"must":[
{"range":{"an_age":{"from":28, "to":28}}}]}}}





{"query":{"bool":{"must":[
{"range":{"blf_ygd_loan_amt_all":{"gte":0}}}]}},
  "_source": ["rf_bank_all"]}


{"query": {"bool": {"must": [
  {"range": {"blf_yfq_loan_amt_all": {"gte": 1.0}}},
  {"regexp":{"rf_bank_all":"[,]?交通银行[,]?"}}
  ]}},
  "_source": ["rf_bank_all"]}

{"query":{"regexp":{"rf_bank_all":"[,]?交通银行[,]?"}},
  "_source": ["rf_bank_all"]}

//=============terms操作====================
{"query":{"bool":{"must":[
{ "terms" : {"pk_mobile":["13717518671", "15042335577"]}}
]}
},
  "_source": ["pk_mobile"]
}

{"query": {"bool": {"must": [{"term":{"bh_blp_ltrm_cmax": "公共网点"}}]}}}


{"query":{"regexp":{"rf_bank_all":"[,]?交通银行[,]?"}},
  "_source": ["bh_blp_ltrm_cmax"]}



{"query":{"bool":{"must":[
{"terms":{"blm_prov_cmax_6m":["江苏", "辽宁省"]}}]}
},
"_source":["bh_blp_ltrm_cmax","blm_prov_cmax_6m"]
}


{"query": {"bool": {"must": [{"terms": {"bh_blp_ltrm_cmax": ["手刷"]}}]}},
"_source":["bh_blp_ltrm_cmax","blm_prov_cmax_6m"]}


{"query": {"bool": {"must": [{"query_string" : {"default_field" : "bh_blp_ltrm_cmax","query" : "拉卡拉IC卡手刷","phrase_slop":1}}]}},
"_source":["bh_blp_ltrm_cmax","blm_prov_cmax_6m"]}




//=============multi match================
{"query":{"bool":{"must":[
{ "match_phrase" : {"query":["13717518671", "15042335577"], 
   "fields":"pk_mobile"}}
]}
},
  "_source": ["pk_mobile"]
}

{"query":{"match_phrase":{"pk_mobile":{"query":["13717518671", "15042335577"],
  "slop":1
  }}}
}

{"query": {"bool": {"must": [{"query_string" : {"default_field" : "bh_blp_ltrm_cmax","query" : "拉卡拉IC卡手刷"}}]}},
"_source":["bh_blp_ltrm_cmax","blm_prov_cmax_6m"]}


{"query": {"bool": {"must": [{"match_phrase" : {"field" : "bh_blp_ltrm_cmax","query" : "拉卡拉IC卡手刷","slop":1}}]}},
"_source":["bh_blp_ltrm_cmax","blm_prov_cmax_6m"]}


{"query": {"bool": {"should": [{"regexp": {"rf_bank_all": "[,]?\u5efa\u8bbe\u94f6\u884c[,]?"}}]}}}

{"query": {"bool": {"must": [{"should": [{"regexp": {"rf_bank_all": "[,]?\u5efa\u8bbe\u94f6\u884c[,]?"}}]},{"terms":{"blm_prov_cmax_6m":["江苏", "辽宁省"]}}]}}}




//=================(and) and (or)操作======================
{"query":{
  "filtered":{
    "filter":{
      "and":[{"regexp": {"rf_bank_all": "[,]?\u5efa\u8bbe\u94f6\u884c[,]?"}}]
    },
    "query":{
      "bool":{
        "must":[{"terms":{"blm_prov_cmax_6m":["江苏", "辽宁省"]}}]
      }
    }
  }
}
}

{"query":{
  "filtered":{
    "filter":{
      "and":[{"bool":{"must":[
                {"range":{"an_age":{"from":0, "to":100}}}]}
                },
             {"or":[{"regexp": {"rf_bank_all": "[,]?\u5efa\u8bbe\u94f6\u884c[,]?"}}]}]
    },
    "query":{
      "bool":{
        "must":[{"terms":{"blm_prov_cmax_6m":["江苏", "辽宁省"]}}]
      }
    }
  }
}
}

{"query":{
  "filtered":{
    "filter":{
      "and":[{"bool":{"must":[
                {"range":{"an_age":{"from":-99, "to":100}}}]}
                },
             {"or":[{"regexp": {"rf_bank_all": "[,]?\u5efa\u8bbe\u94f6\u884c[,]?"}}]}]
    },
    "query":{
      "bool":{
        "must":[{"terms":{"blm_prov_cmax_6m":["江苏", "辽宁省"]}}]
      }
    }
  }
}
}



{"query": {"filtered": {"filter": {"and": [{"or": [{"regexp": {"rf_cob_all": "[,]?\u4e2d\u56fd\u94f6\u884c[,]?"}}, {"regexp": {"rf_cob_all": "[,]?\u4e2d\u56fd\u94f6\u884c\uff08\u9999\u6e2f\uff09[,]?"}}, {"regexp": {"rf_cob_all": "[,]?\u4e2d\u884c\u65b0\u52a0\u5761\u5206\u884c[,]?"}}, {"regexp": {"rf_cob_all": "[,]?\u4e2d\u56fd\u94f6\u884c\u6fb3\u95e8\u5206\u884c[,]?"}}]}]}}},
"_source":["rf_cob_all"]}


"[,]?交通银行[,]?"
{"query":{
  "filtered":{
    "filter":{
      "and":[
             {"or":[{"regexp": {"rf_bank_all": ".*?[,]?交通银行[,]?.*"}}]}]
    },
  "query":{"bool":{"must":[
    {"query_string" : {"default_field" : "pk_mobile","query" : "13717518671 OR 15042335577 OR 15024278110","phrase_slop":1}}
  ]}
  }
  
  }
},
  "_source": ["pk_mobile", "rf_cob_all"]
}


{"query":{"bool":{"must":[
{"query_string" : {"default_field" : "pk_mobile","query" : "13717518671 OR 15042335577","phrase_slop":1}}
]}
},
  "_source": ["pk_mobile", "rf_cob_all"]
}


{"query":{"filtered":{"filter":{"missing":{"field":"blr_xykhk_cob_cnt_24m"}}}}}


{"query":{
  "filtered":{
    "filter":{
      "or":[{"bool":{"must":[
                {"range":{"blr_xykhk_cob_cnt_24m":{"from":-99, "to":0}}}]}
                },
              {"missing":{"field":"blr_xykhk_cob_cnt_24m"}}]
    }
  }
}
}

{"query":{
  "filtered":{
    "filter":{
      "or":[{"regexp": {"rf_bank_all": "[,]?\u5efa\u8bbe\u94f6\u884c[,]?"}},{"missing":{"field":"rf_bank_all"}}]
    },
    "query":{
      "bool":{
        "must":[{"terms":{"blm_prov_cmax_6m":["江苏", "辽宁省"]}}]
      }
    }
  }
}
}


{"query":{
  "filtered":{
    "filter":{
      "or":[{"bool":{"must_not":[{"regexp": {"rf_bank_all": "[,]?\u5efa\u8bbe\u94f6\u884c[,]?"}}]}},{"missing":{"field":"rf_bank_all"}}]
    }
  }
}
}

{"query": {"filtered": {"filter": {"and": [{"or": [{"bool": {"must": [{"terms": {"rf_cc_bl": ["1", "0"]}}]}}, {"missing": {"field": "bw_blp_ss_bl"}}]}]}}}}



import org.elasticsearch.spark.sql._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
val data = sqlContext.sql("SELECT * FROM uts.ulb_collect_all_sample")
data.saveToEs(Map(ES_RESOURCE_WRITE -> "label/test", ES_NODES -> "10.1.80.75",
            ES_MAPPING_ID -> "pk_mobile"))

curl http://10.1.80.75:9200/label/test2/_cat/count

val data = sqlContext.sql("SELECT * FROM uts.ulb_all_dupd_m where ymd=20160518 limit 200000")
data.saveToEs(Map(ES_RESOURCE_WRITE -> "label_1/ulb_collect_all", ES_NODES -> "10.1.80.75",
    ES_MAPPING_ID -> "pk_mobile"))


val tedata = data.where("pk_mobile in ('13801380616','13801021969','13901154251','13911890501','13907640690','13602692360','13601616757','13911719371','13801255252','18911402345','13717518671','15810628105','15210637902','13146055559','13810067988','18911186260')")

val fen2yuanStr = """blr_xykhk_amt_3m,blr_xykhk_amt_12m,blr_hfcz_amt_3m,blr_hfcz_amt_12m,blr_zzhk_amt_2m,blr_zzhk_amt_12m,rf_lqb_acbal,rf_lfp_acbal,rf_lkd_acbal,blf_qb_acbal_amt_lst,blf_qb_acbal_amt_max_all,blf_kd_acbal_amt_lst,blf_kd_acbal_amt_max_all,blf_kd_acin_amt_fst,blf_kd_acin_amt_lst,blf_kd_acout_amt_fst,blf_kd_acout_amt_lst,blf_lfp_acbal_amt_lst,blf_lfp_acbal_amt_max_all,blf_lfp_acin_amt_fst,blf_lfp_acin_amt_lst,blf_lfp_acout_amt_fst,blf_lfp_acout_amt_lst,blf_tnh_loan_amt_all,blf_yfq_loan_amt_all,blf_ygd_loan_amt_all,blf_tnh_loan_amt_lst,blf_yfq_loan_amt_lst,blf_ygd_loan_amt_lst,blf_tnh_repayf_amt,blf_yfq_repayf_amt,blf_ygd_repayf_amt,blf_qb_acbal_amt_mavg_all,blf_qb_acbal_amt_mavg_3m,blf_qb_acbal_amt_mavg_6m,blf_qb_acbal_amt_mavg_12m,blf_qb_acbal_amt_max_3m,blf_qb_acin_amt_mavg_all,blf_qb_acin_amt_mavg_3m,blf_qb_acin_amt_mavg_6m,blf_kd_acbal_amt_mavg_all,blf_kd_acbal_amt_mavg_3m,blf_kd_acbal_amt_mavg_6m,blf_kd_acbal_amt_mavg_12m,blf_kd_acbal_amt_max_3m,blf_lfp_acbal_amt_mavg_all,blf_lfp_acbal_amt_mavg_3m,blf_lfp_acbal_amt_mavg_6m,blf_lfp_acbal_amt_mavg_12m,blf_lfp_acbal_amt_max_3m,blf_tnh_loan_amt_6m,blf_tnh_loan_amt_12m,blf_tnh_loan_max_amt_12m,blf_yfq_loan_amt_6m,blf_yfq_loan_amt_12m,blf_yfq_loan_max_amt_12m,blf_ygd_loan_amt_6m,blf_ygd_loan_amt_12m,blf_ygd_loan_max_amt_12m,blf_tnh_loan_amt_all,blf_yfq_loan_amt_all,blf_ygd_loan_amt_all,blf_tnh_repayf_amt,blf_yfq_repayf_amt,blf_ygd_repayf_amt"""
val yuanDF = fen2yuan(tedata, fen2yuanStr)

val desField = "as_idc"
val desDF = desensitization(yuanDF, desField)

desDF.saveToEs(Map(ES_RESOURCE_WRITE -> "label_1/ulb_collect_all", ES_NODES -> "10.1.80.75",
    ES_MAPPING_ID -> "pk_mobile"))



import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StringType,StructField,DoubleType,IntegerType}

val data = sqlContext.sql("SELECT * FROM uts.ulb_all_dupd_m where ymd=20160518")
val ndata = data.map{row=>
      Row(row(0), row.toSeq.filter(_==null).length)
    }


ndata.cache
ndata.first

val myschema = StructType(Array(StructField("pk_mobile",StringType,true), StructField("num", IntegerType, true)))

val myDF = sqlContext.createDataFrame(ndata, myschema)



myDF.filter("num < 200").count

val a = myDF.filter("num < 150").collect

a.foreach(println(_))


员工贷至今审核未通过++

{"query": {"filtered": {"filter": {"and": [{"or": [{"bool": {"must": [{"terms": {"blf_ygd_audit_f": ["1", "0"]}}]}}, {"missing": {"field": "blf_ygd_audit_f"}}]}]}}}}

{"query": {"filtered": {"filter": {"and": [{"or": [{"bool": {"must": [{"terms": {"blf_lfp_acin_chan_lst": ["lakala", "lakalaam", "lakaladt", "lakalaeo", "lakalaol", "lakalasa", "lakalasd", "lakalawx", "lakalayd", "lakalazj", "lklnfczf"]}}]}}]}]}}}}

{
    "query" : {
        "filtered" : {
            "filter": {
                "exists" : { "field" : "blf_kd_acin_way_fst" }
            }
        }
    },
"_source":"blf_lfp_acin_chan_lst"
}

{"query":{"bool":{"must":[
{"query_string" : {"default_field" : "blf_kd_acin_way_fst","query" : "\u7b7e\u7ea6\u5361","phrase_slop":1}}
]}
},
  "_source": ["blf_kd_acin_way_fst"]
}



{"query":{"bool":{"must":[
{"range":{"blr_yecx_amt_cmax_6m":{"from":"0", "to":"632260000"}}},
{"range":{"blr_yecx_dc_cnt_24m":{"from":"0", "to":"63"}}},
{"range":{"blf_qb_bc_date_fst":{"gt":"2012-06-31"}}},
{"range":{"blf_qb_bc_date_lst":{"gt":"2012-06-31"}}},
{"range":{"blf_qb_bc_date_lst":{"lt":"2016-02-28"}}},
{"query_string" : {"default_field" : "as_area_hukou_prov","query" : "湖南 OR 北京 OR 广东 OR 山西 OR 上海"}},
{"query_string" : {"default_field" : "as_area_live","query" : "北京 OR 广东 OR 上海"}},
{"query_string" : {"default_field" : "as_mail","query" : "163.com"}},
{"term":{"an_gender":"男"}},
{"term":{"an_zodiac":"龙"}},
{"term":{"blm_prov_cmax_6m":"江苏"}},
{"term":{"blm_prov_cmax_9m":"江苏"}},
{"term":{"as_marital":"已婚"}}]}
}
}


{
  "from":0,
  "size":100,
  "query":{"bool":{"must":[
{ "term" : {"user_id":"1000122545"}}
]}
},
  "sort":{"data_date":{"order":"desc"}}
}

{
  "query":{"match_all":{}},
  "sort":{"data_date":{"order":"desc"}}
}


{"from":0,"size":100,"query":{"bool":{"must":[{ "term" : {"user_id":"1000122545"}}]}},
  "sort":{"data_date":{"order":"desc"}}}


//Elasticsearch排序
{"aggs":{
  "mobile_num":{
    "terms":{
      "field":"mobile_num",
      "order":[
        {
          "_count":"asc"
        }
      ],
      "size":2
    },
    "aggs":{}
  }
  },
  "size":0
}




{"from":0,"size":100,"query":{"bool":{"must":[{ "term" : {"mobile_num":13801255252}}]}}}


import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, DataFrame, Row}

val raw = sc.textFile("/user/hive/warehouse/hdw.db/lklpos_atmtxnjnl/ymd=20160628")
val sites = Array(6, 8, 12, 24)
val rdd_row = raw.map{line=>
    val fields = line.stripLineEnd.split("\001")
    val re = sites.map(fields(_))
    Row.fromSeq(re.toSeq)
}

val fieldNames = "trans_date,trans_name,cardno1,total_am"
val schemaArr = fieldNames.split(",").map(field => StructField(field, StringType, true))
val schema = StructType(schemaArr)

val pos_df = sqlContext.createDataFrame(rdd_row, schema)

val carArr = rdd_row.map(r=>r(2))

carArr.filter(r=>r.toString=="").count


val c = Array("a", "b", "c", "b", "a", "a")
val c_rdd = sc.parallelize(c).map(w => (w, ArrayBuffer(1,2)))

val group = c_rdd.groupByKey().values.map{l => l.flatMap(i => i.toList)}

group.collect.foreach(println(_))

val t = rdd2.first
t.find(i=>i.toString.startsWith("Gen"))
t.indexOf("Gene")


val rdd1 = partitionRow2Col(rdd)
val rdd2 = combinPartition(rdd1)
rdd2.saveAsTextFile("/data/mllib/brca/afterRow2Col")
val result = label(rdd2, kv)


val saved = result.map{ lp=> 
    var labelFeatures = new Array[Double](0)
    labelFeatures = labelFeatures :+ lp.label
    labelFeatures = labelFeatures ++ lp.features.toArray
    labelFeatures.mkString("\001") + "\n" 
}
saved.saveAsTextFile("/data/mllib/brca/labelPoint")


import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.DenseVector
val datalp = sc.textFile("/data/mllib/brca/labelPoint", 5).map{ line=>
    var a = LabeledPoint(0.0, new DenseVector(Array(0.0)))
    try{
      val tmp = line.trim.split("\001").map(i=>i.toDouble)
      a = LabeledPoint(tmp(0), new DenseVector(tmp.slice(1, tmp.length)))
    }catch{
      case _:NumberFormatException => println(line)
    }
    a
}
val datalp1 = datalp.filter(lp => lp.features.size==20531)


import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.Node
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, FeatureType, Strategy}

val numTrees = 10

val boostingStrategy = BoostingStrategy.defaultParams("Classification")
boostingStrategy.setNumIterations(numTrees)      //设置树的多少
val treeStrategy = Strategy.defaultStrategy("Classification")
treeStrategy.setNumClasses(2)
treeStrategy.setMaxDepth(30)
//    treeStrategy.setCategoricalFeaturesInfo(Map[Int, Int]())
boostingStrategy.setTreeStrategy(treeStrategy)

val splits = datalp1.randomSplit(Array(0.7, 0.3), 1)
val (trainData, test1Data) = (splits(0), splits(1))
// val length = trainData.map(lp => lp.features.size).toArray
val model = GradientBoostedTrees.train(trainData, boostingStrategy)

val labelAndPred = test1Data.map{ lp =>
  (lp.label, model.predict(lp.features))
}

val treeLeafArray = new Array[Array[Int]](numTrees)
for(i <- 0 until numTrees){
  treeLeafArray(i) = getLeafNodes(model.trees(i).topNode)
}


import org.apache.spark.rdd.RDD
val newFeatureData = (data:RDD[LabeledPoint]) => data.map{ lp =>      //匿名函数用于转换特征
  var newFeature = new Array[Double](0)
  for(i <- 0 until numTrees){
    val nodeId = predictModify(model.trees(i).topNode, lp.features.toDense)
    val leafArray = new Array[Double]((model.trees(i).numNodes+1)/2)
    leafArray(treeLeafArray(i).indexOf(nodeId)) = 1
    newFeature = newFeature ++ leafArray
  }
  LabeledPoint(lp.label, new DenseVector(newFeature))
}


val newTrainData = newFeatureData(trainData)
val newTestData = newFeatureData(test1Data)



import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics

val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(newTrainData)
val predAndLabel = newTestData.map{ lp =>
  (model.predict(lp.features), lp.label)
}
val metrics = new MulticlassMetrics(predAndLabel)
val precision = metrics.precision
println("Precision = " + precision)

val errorRatio = predAndLabel.filter(lp=>lp._1!=lp._2).count.toDouble / predAndLabel.count
val pn = datalp1.filter(lp=>lp.label==0.toDouble).count.toDouble/datalp1.filter(lp=>lp.label==1.toDouble).count

//1代表NT, 0代tumor
//检测将tumor变为NT的比例
val falseNegative = predAndLabel.filter(lp=>lp._1==1D && lp._2==0D).count.toDouble/predAndLabel.count  
//0.026881720430107527  变换前    0.022222222222222223变换后
val fN = predAndLabel.filter(lp=>lp._1==1D && lp._2==0D).count.toDouble/predAndLabel.filter(lp=>lp._1==1D).count

val tN = predAndLabel.filter(lp=>lp._1==1D && lp._2==0D).count.toDouble/predAndLabel.filter(lp=>lp._1==1D).count



{
  "from":0,
  "size":100,
  "query":{"bool":{"must":[
{ "term" : {"count":1}}
]}
},
  "sort":{"@timestamp":{"order":"desc"}}
}

hadoop02_hadoop-cmf-yarn-RESOURCEMANAGER-hadoop02encrypted001logencrypted001out

curl -XPUT 'http://10.1.80.75:9200/hadoop02_hadoop-cmf-yarn-RESOURCEMANAGER-hadoop02encrypted001logencrypted001out/'


curl -XPUT 'http://10.1.80.75:9200/hadoop02_hadoop-cmf-yarn-resourcemanager-hadoop02encrypted001logencrypted001out/'

"appMasterHost": "hadoop10",


{"query":{"bool":{"must":{"query_string" : {"default_field" : "appMasterHost","query" : "hadoop10"}}}}}

{"query":{"bool":{"must":{"query_string" : {"default_field" : "finalStatus","query" : "SUCCEEDED"}}}}}

{"query":{"bool":{"must_not":{"query_string" : {"default_field" : "finalStatus","query" : "SUCCEEDED"}}}}}




{"query":{"filtered":{"filter":{"missing":{"field":"appId"}}}}}



sudo -u hdfs spark-submit --master yarn --name job_container_info --class LogsMain --num-executors 10 --jars elasticsearch-hadoop-2.3.1.jar RealTimeLogs.jar hadoop02_hadoop-cmf-yarn-RESOURCEMANAGER-hadoop02encrypted001logencrypted001out yarn/test1

val arr = Array(1,2,3,4)
val a = sc.parallelize(arr)
val b = sc.parallelize(arr)

val rdds = Array(a, b)

val c = rdds.reduce{
  (a, b) =>{
    a.union(b)
  }
}



spark-submit --master yarn --name yxt --num-executors 10 --jars /home/dyh/softwares/elasticsearch-hadoop-2.3.1.jar labelHistoryZipTable.jar yxt.cp_trans_success_tele yxt/cp_trans_success_tele special 20121231,20131231,20141231,20151231


spark-submit --master yarn --name yxt --num-executors 10 --jars /home/dyh/softwares/elasticsearch-hadoop-2.3.1.jar labelHistoryZipTable.jar yxt.cp_trans_success_tele yxt/cp_trans_success_tele increment 20160826

hadoop08: /home/hdfs/demands/cp_trans_success_tele.sh   配置

{"query":{"bool":{"must":[
{"range":{"trans_date":{"gte":"now-14d"}}},
{"term":{"mobile_num":"15236389689"}}]}
}
}

{"query":{"bool":{"must":[
{"range":{"trans_date":{"gte":"now-7d"}}}]}
}
}


{ "from" : 0, "size" : 10, "query" : { "bool" : { "must" : [ { "range" : { "trans_date" : { "from" : "now-3y", "to" : null, "include_lower" : true, "include_upper" : true } } }, { "term" : { "mobile_num" : "13907640690" } } ] } }, "sort" : [ { "trans_date" : { "order" : "desc" } } ] } 


{"query":{
  "filtered":{
    "filter":{
      "and":[{
        "term" : {
          "mobile_num" : "13911039405"
        }
      }]
    },
    "query":{
      "bool":{
        "must":[{
        "range" : {
          "trans_date" : {
            "from" : "now-3y",
            "to" : null,
            "include_lower" : true,
            "include_upper" : true
          }
        }
      } ]
      }
    }
  }
},
  "sort" : [ {
    "trans_date" : {
      "order" : "desc"
    }
  } ]
}






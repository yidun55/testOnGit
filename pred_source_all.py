from pyspark import SparkConf, SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import SparseVector, Vectors
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
import numpy as np
from decimal import Decimal
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

conf = SparkConf().setAppName("pred_source_all").setMaster("local")
#sc = SparkContext(appName='pred_source_all')
sc = SparkContext(conf=conf)

def drop_na(x):
    """
    drop the columns containing NA
    """
    a = x.strip().split("\001")
    filtered_data = [a[i] for i in xrange(len(a)) if i not in na_col.value]
    indice = range(len(filtered_data)-1)
    #return float(filtered_data[0]), np.array(indice, dtype=np.int32), np.array(filtered_data[1:],dtype=np.float64)
    return filtered_data[0], np.array(indice, dtype=np.int32), np.array([el.strip() for el in filtered_data[1:]],dtype=np.dtype(Decimal))

data = sc.textFile("/data/mllib/pred_source_all_com_small")
#pred_data = sc.textFile("/data/mllib/test_source_all_clean_small")
pred_data = sc.textFile("/data/mllib/std_skb_test_0901")

#broadcast the columns containing NA
na_col = sc.broadcast(range(80,83)+[84]+range(87,92)+[97])


#drop out columns with NA
parsed_data = data.map(drop_na)

numFeatures = -1
if numFeatures <= 0:
    parsed_data.cache()
    #numFeatures = parsed_data.map(lambda x:-1 if x[1].size==0 else x[1][-1]).reduce(max)+1
    numFeatures = parsed_data.map(lambda x:-1 if len(x[1])==0 else x[1][-1]).reduce(max)+1
#parsed_data.cache()
#labeled_data = parsed_data.map(lambda x:LabeledPoint(x[0], x[1]))
labeled_data = parsed_data.map(lambda x:LabeledPoint(x[0], Vectors.sparse(numFeatures, x[1], x[2])))
trainData, testData = labeled_data.randomSplit([0.5, 0.5])

#broadcast the columns containing NA
na_col = sc.broadcast(range(80,83)+[84]+range(87,92)+[97])

#drop out columns with NA
pred_parsed_data = pred_data.map(drop_na)

#numFeatures = -1
#if numFeatures <= 0:
#    pred_parsed_data.cache()
#    numFeatures = pred_parsed_data.map(lambda x:-1 if x[1].size==0 else x[1][-1]).reduce(max)+1
pred_parsed_data.cache()
pred_labeled_data = pred_parsed_data.map(lambda x:LabeledPoint(x[0], Vectors.sparse(101, x[1], x[2])))


model = RandomForest.trainClassifier(trainData,numClasses=2, categoricalFeaturesInfo={}, numTrees=200, featureSubsetStrategy="auto", impurity='gini', maxDepth=15,maxBins=32)

#model = GradientBoostedTrees.trainClassifier(trainData, categoricalFeaturesInfo={}, numIterations=3) 

pred = model.predict(pred_labeled_data.map(lambda x:x.features))
labelsAndPredictions = pred_labeled_data.map(lambda lp: lp.label).zip(pred)
error = labelsAndPredictions.filter(lambda (v,p):v!=p).count()/float(pred_labeled_data.count())
print 'pred_error =', str(error), "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
ture_positive = labelsAndPredictions.filter(lambda (v,p):v==p and v==1).count()/float(labelsAndPredictions.filter(lambda (v,p):p==1).count())
print "precision = TP/(TP+FP) ", ture_positive,"**************************************************************************"

ture_Flase = labelsAndPredictions.filter(lambda (v,p):v==1).count()/float(labelsAndPredictions.count())
print "ture flase", ture_Flase, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"



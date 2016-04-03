# Run a random forrest classification model on the data las vegas dataset
# print relavant stats

from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("KMeans")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#dfModelDataAll = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/home/ub51/SFU/733Project/Data/model.csv')
#dfModelDataAll.show()

#dfnoID = dfModelDataAll.select('yelp_score','sum_anger','sum_happy','sum_conf','sum_disgust','no_vio','risk_rating')

#dfnoID.show()

data_loc = "lv_cluster_long.parquet"
data_df = sqlContext.parquetFile(data_loc)

(trainingData, testData) = data_df.randomSplit([0.7, 0.3])

trainLab = trainingData.map(lambda l:LabeledPoint(l.cluster, l.vector))

testLab = testData.map(lambda l:LabeledPoint(l.cluster, l.vector))


model = RandomForest.trainClassifier(trainLab, numClasses=3,\
                     categoricalFeaturesInfo={},  numTrees=3,\
                              impurity='entropy')
print "Model Ready"

predictions = model.predict(testLab.map(lambda x: x.features))

labelsAndPredictions = testLab.map(lambda lp: lp.label).zip(predictions)

finalRdd = testData.map(lambda rec:(rec.yelp_id,rec.stars)).zip(predictions) 

testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testLab.count())
print('Test Error = ' + str(testErr))
print('Learned classification forest model:')
print(model.toDebugString())

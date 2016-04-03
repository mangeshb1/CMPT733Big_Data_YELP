from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint

dfModelDataAll = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/home/ub51/SFU/733Project/Data/model.csv')
dfModelDataAll.show()

#dfnoID = dfModelDataAll.select('yelp_score','sum_anger','sum_happy','sum_conf','sum_disgust','no_vio','risk_rating')

#dfnoID.show()

(trainingData1, testData) = dfModelDataAll.randomSplit([0.7, 0.3])

trainingData = trainingData1.select('yelp_score','sum_anger','sum_happy','sum_conf','sum_disgust','no_vio','risk_rating')

trainLab = trainingData.map(lambda l:LabeledPoint(l.risk_rating,[l.yelp_score,l.sum_anger,l.sum_happy,l.sum_conf,l.sum_disgust,l.no_vio]))

testLab = testData.map(lambda l:LabeledPoint(l.risk_rating,[l.yelp_score,l.sum_anger,l.sum_happy,l.sum_conf,l.sum_disgust,l.no_vio]))


model = RandomForest.trainClassifier(trainLab, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=3, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth=4, maxBins=32)
print "Model Ready"

predictions = model.predict(testLab.map(lambda x: x.features))

labelsAndPredictions = testLab.map(lambda lp: lp.label).zip(predictions)

finalRdd = testData.map(lambda rec:(rec.yelp_id,rec.yelp_score)).zip(predictions) 

testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testLab.count())
print('Test Error = ' + str(testErr))
print('Learned classification forest model:')
print(model.toDebugString())
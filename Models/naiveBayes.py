# NaiveBayes
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.evaluation import MulticlassMetrics

dfModelDataAll = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/home/ub51/SFU/733Project/Data/model.csv')
dfModelDataAll.show()

#dfnoID = dfModelDataAll.select('yelp_score','sum_anger','sum_happy','sum_conf','sum_disgust','no_vio','risk_rating')

#dfnoID.show()

(trainingData1, testData) = dfModelDataAll.randomSplit([0.7, 0.3])

trainingData = trainingData1.select('yelp_score','sum_anger','sum_happy','sum_conf','sum_disgust','no_vio','risk_rating')

trainingData.show()
testData.show()

trainLab = trainingData.map(lambda l:LabeledPoint(l.risk_rating,[l.yelp_score,l.sum_anger,l.sum_happy,l.sum_conf,l.sum_disgust,l.no_vio]))

testLab = testData.map(lambda l:LabeledPoint(l.risk_rating,[l.yelp_score,l.sum_anger,l.sum_happy,l.sum_conf,l.sum_disgust,l.no_vio]))

model = GradientBoostedTrees.trainClassifier(trainLab,
                                             categoricalFeaturesInfo={}, numIterations=3)

print "Model Ready"

predictions = model.predict(testLab.map(lambda x: x.features))

labelsAndPredictions = testLab.map(lambda lp: lp.label).zip(predictions)

finalRdd = testData.map(lambda rec:(rec.yelp_id,rec.yelp_score)).zip(predictions) 

testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testLab.count())
print('Test Error = ' + str(testErr))
print('Learned classification forest model:')
print(model.toDebugString())

#Code for Spark Evaluation Matrix

metrics = MulticlassMetrics(labelsAndPredictions)
# Overall statistics
precision = metrics.precision()
recall = metrics.recall()
f1Score = metrics.fMeasure()
confusionMatrx = metrics.confusionMatrix()

print("All Evaluation Measures Stats")
print("Confusion Matix = %s" % confusionMatrx)
print("Precision = %s" % precision)
print("Recall = %s" % recall)
print("F1 Score = %s" % f1Score)

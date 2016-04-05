# Decision Tree 
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.mllib.linalg import SparseVector
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext,SparkConf
from pyspark.mllib.evaluation import MulticlassMetrics

conf = SparkConf().setAppName("KMeans")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#dfModelDataAll = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/home/ub51/SFU/733Project/Data/model.csv')
#dfModelDataAll.show()

#dfnoID = dfModelDataAll.select('yelp_score','sum_anger','sum_happy','sum_conf','sum_disgust','no_vio','risk_rating')

#dfnoID.show()

data_loc = "../lv_data/lv_class_input.parquet"
data_df = sqlContext.parquetFile(data_loc)

(trainingData, testData) = data_df.randomSplit([0.7, 0.3])

trainLab = trainingData.map(lambda l:LabeledPoint(l.risk, l.vector))

testLab = testData.map(lambda l:LabeledPoint(l.risk, l.vector))


model = DecisionTree.trainClassifier(trainLab, numClasses=3, categoricalFeaturesInfo={},
                    impurity='entropy', maxDepth=10, maxBins=32)

print "Model Ready"

predictions = model.predict(testLab.map(lambda x: x.features))

labelsAndPredictions = testLab.map(lambda lp: lp.label).zip(predictions)
labelsAndPredictions = labelsAndPredictions.map(lambda (a,b): (b,a))

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

print ("DECISION TREE FOR TEST SET ONLY")
print("All Evaluation Measures Stats")
print("Confusion Matix = %s" % confusionMatrx)
print("Precision = %s" % precision)
print("Recall = %s" % recall)
print("F1 Score = %s" % f1Score)


#now perform decision tree on all data
testLab = data_df.map(lambda l:LabeledPoint(l.risk, l.vector))

predictions = model.predict(testLab.map(lambda x: x.features))

labelsAndPredictions = testLab.map(lambda lp: lp.label).zip(predictions)
labelsAndPredictions = labelsAndPredictions.map(lambda (a,b): (b,a))


finalRdd = data_df.map(lambda rec:(rec.yelp_id, \
                    rec.name, rec.vector[0], rec.risk)).zip(predictions) 

testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testLab.count())
print('Test Error = ' + str(testErr))
print('Learned classification forest model:')
print(model.toDebugString())

metrics = MulticlassMetrics(labelsAndPredictions)
# Overall statistics
precision = metrics.precision()
recall = metrics.recall()
f1Score = metrics.fMeasure()
confusionMatrx = metrics.confusionMatrix()

print("DECISION TREE FOR ALL DATA: ")
print("All Evaluation Measures Stats")
print("Confusion Matix = %s" % confusionMatrx)
print("Precision = %s" % precision)
print("Recall = %s" % recall)
print("F1 Score = %s" % f1Score)

#save output
#finalRdd.coalesce(1).saveAsTextFile("decision_tree_output73")


#run on the boston data:
boston = sqlContext.parquetFile("../boston_data/boston_vectors")
predictions = model.predict(boston.map(lambda x: x.vector))



finalRdd = boston.map(lambda rec:(rec.yelp_id)).zip(predictions) 

final_df = sqlContext.createDataFrame(finalRdd, ["yelp_id", "risk"])
final_df.write.parquet("boston_classified", mode='overwrite')
final_df.show()

from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt


dfModelDataAll = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/home/ub51/SFU/733Project/Data/model.csv')
dfModelDataAll.show()
# Load and parse the data

trainingData = dfModelDataAll.select('yelp_score','sum_anger','sum_happy','sum_conf','sum_disgust','no_vio','risk_rating')

trainParse = trainingData.map(lambda l:[l.yelp_score,l.sum_anger,l.sum_happy,l.sum_conf,l.sum_disgust,l.no_vio])

#features = df1.select("features").rdd.map(lambda row: row[0]).cache()

ptWithYelp = dfModelDataAll.map(lambda l:(l.yelp_id,[l.yelp_score,l.sum_anger,l.sum_happy,l.sum_conf,l.sum_disgust,l.no_vio]))


# Build the model (cluster the data)
clusters = KMeans.train(trainParse, 3, maxIterations=10,
        runs=10, initializationMode="random")

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = trainParse.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))


#predictUDF = udf(lambda x: modelBC.value.predict(x), StringType())
#dfModelDataAll = df1.withColumn("prediction", predictUDF(df1.features)).cache()

myFinalData = ptWithYelp.map(lambda (a,c):(a,c,clusters.predict(c)))

print myFinalData.collect()

# Save and load model
#clusters.save(sc, "myModelPath")
#sameModel = KMeansModel.load(sc, "myModelPath")

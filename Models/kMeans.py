# Take the groupby dataset
# And perfrom k means clustering to separate the data into 3 groups
# Then discern the 3 groups in high, low, and medium risk classes

from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import udf, col
from pyspark.mllib.stat import Statistics

conf = SparkConf().setAppName("KMeans")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#dfModelDataAll = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/home/ub51/SFU/733Project/Data/model.csv')
#dfModelDataAll.show()
# Load and parse the data

#groupby data
df_loc = "../sentiment_analysis/aggregated_senti.parquet"
dfModelDataAll = sqlContext.parquetFile(df_loc)

#yelp business data set to get yelp score
yelp_bus_loc = "../yelp_data/yelp_academic_dataset_business.json"
yelp_df = sqlContext.read.json(yelp_bus_loc).select("business_id", "stars")

#join to get yelp score
dfModelDataAll = dfModelDataAll.join(yelp_df, \
                   yelp_df.business_id==dfModelDataAll.yelp_id)\
                   .drop(yelp_df.business_id)

#dfModelDataAll.show(20, False)
#raise SystemExit

#select k means parameters
trainingData = dfModelDataAll.select('stars', 'avg_anger', 'avg_disgust', \
                        'avg_fear', 'avg_joy', 'avg_sadness',\
                        'avg_agree', 'avg_emotion', 'vio_per_year',\
                        'four_scale_stars', 'five_scale_stars')

#trainParse = trainingData.map(lambda l:[l[0], l[1], l[2], l[3], \
#                                        l[4], l[5], l[6], l[7], l[8], \
#                                            l[9], l[10]])

ptWithYelp = dfModelDataAll.map(lambda l:(l.yelp_id, l.lv_id, l.name,\
                                          [l.stars,\
                                          1.0 * l.five_scale_stars,\
                                          1.0 * l.four_scale_stars,\
                                          l.avg_disgust,\
                                          l.avg_agree,\
                                          l.avg_emotion,\
                                          l.avg_sadness,\
                                          l.avg_anger,\
                                          l.avg_joy]))

#adding fear and vio_per_year makes things worse

trainParse = ptWithYelp.map(lambda (yid, lid, n, vec): vec)
summary = Statistics.colStats(trainParse)
print("MEAN: ", summary.mean())
print("STD: ", summary.variance())
#raise SystemExit


# Build the model (cluster the data)
clusters = KMeans.train(trainParse, 3, maxIterations=100)

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = trainParse.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))


#predictUDF = udf(lambda x: modelBC.value.predict(x), StringType())
#dfModelDataAll = df1.withColumn("prediction", predictUDF(df1.features)).cache()

myFinalData = ptWithYelp.map(lambda (yid, lvid, n, vec):(yid, lvid, n, vec,\
                                                 clusters.predict(vec)))

myFinalData = sqlContext.createDataFrame(myFinalData,\
                     ['yelp_id', 'lv_id', 'name', 'vector', 'cluster'])

# Save and load model
#clusters.save(sc, "myModelPath")
#sameModel = KMeansModel.load(sc, "myModelPath")
myFinalData.write.parquet("lv_cluster_long.parquet", mode='overwrite')

a1 = myFinalData.where(myFinalData.cluster==0)
a1.show(4, False)
a2 = myFinalData.where(myFinalData.cluster==1)
a2.show(4, False)
a3 = myFinalData.where(myFinalData.cluster==2)
a3.show(4, False)
a4 = myFinalData.where(myFinalData.cluster==3)
a4.show(4, False)
a5 = myFinalData.where(myFinalData.cluster==4)
a5.show(4, False)

print ("COUNT LAST: ", a1.count(), a2.count(), a3.count(), a4.count(), a5.count())

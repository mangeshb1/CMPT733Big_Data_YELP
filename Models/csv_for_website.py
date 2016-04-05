# Make a csv for website sql sever from kmeans results
#
#

from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import udf, col

conf = SparkConf().setAppName("KMeans")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

def risk(r):
    '''convert risk int to str'''

    if r == 0:
        return "Low"
    elif r == 1:
        return "Medium"
    else:
        return "High"

#read data
df_loc = "../lv_data/lv_class_input.parquet"
df = sqlContext.parquetFile(df_loc)

#get score from vector
score_udf = udf(lambda vec: vec[0], FloatType())
risk_udf = udf(lambda c: risk(c), StringType())
df = df.withColumn("score", score_udf(df.vector))
df = df.withColumn("risk", risk_udf(df.risk))

#get vio per year bc it was lost in kmeans
vio_loc = "../sentiment_analysis/aggregated_senti.parquet"
vio_df = sqlContext.parquetFile(vio_loc)
vio_df = vio_df.select(vio_df.lv_id.alias("lv_id2"), "vio_per_year")

#join and select cols
joined_df = df.join(vio_df, vio_df.lv_id2==df.lv_id)
joined_df = joined_df.select("name", "score", "vio_per_year", "risk")

#save to csv
joined_df.repartition(1).write\
    .format('com.databricks.spark.csv').save("lv_website_data3")

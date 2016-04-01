## find one to one lv id to yelp id from fully verified csv
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark import SparkConf, SparkContext
import pylab as pl
import numpy as np
from pyspark.sql.types import *

conf = SparkConf().setAppName("1 2 1 mapping")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

map_df = sqlContext.read.format('com.databricks.spark.csv')\
     .options(header='true', inferschema='true').load("fully_verified.csv")

map_df.registerTempTable("map_df")

m2 = sqlContext.sql("Select lv_id, count(*) as c from map_df group by lv_id")

m2 = m2.where(m2.c==1)
m2 = m2.select(m2.lv_id.alias("lv_id2"))

map_2 = map_df.join(m2, m2.lv_id2==map_df.lv_id)
map_2.registerTempTable("map_2")

m3 = sqlContext.sql("Select yelp_id, count(*) as c from map_2 group by yelp_id")

m3 = m3.where(m3.c==1)
m3 = m3.select(m3.yelp_id.alias("yelp_id2"))

map_3 = map_2.join(m3, m3.yelp_id2==map_2.yelp_id)

#lv_count = map_3.select("lv_id").distinct().count()
#yelp_count = map_3.select("yelp_id").distinct().count()

#print ("COUNTS HERE: ", lv_count, yelp_count)

#save to parquet
map_3 = map_3.select("lv_id", "yelp_id")
map_3.write.format("parquet").save("fully_verified_121_map")

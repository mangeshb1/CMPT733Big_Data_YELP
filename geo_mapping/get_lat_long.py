#  Use the kmeans results and restaurant establishments csv 
#  to find the latitude and longitude of restaurants
#  and save them with the corresponding cluster value into a csv 
#  which will be used to plot them on a map in R

from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark import SparkConf, SparkContext
import pylab as pl
import numpy as np
from pyspark.sql.types import *

conf = SparkConf().setAppName("Make lat long csv")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#read data
lat_long_df = sqlContext.read.format('com.databricks.spark.csv')\
     .options(header='true', inferschema='true')\
     .load("../lv_data/restaurant_establishments.csv")

km_df = sqlContext.parquetFile("../models/lv_cluster_long.parquet")

#join data

joined_df = lat_long_df.join(km_df, km_df.lv_id==lat_long_df.PERMIT_NUMBER)
joined_df = joined_df.select(km_df.cluster, lat_long_df.LATITUDE,\
                                 lat_long_df.LONGITUDE)

#save as csv
joined_df.repartition(1).write\
    .format('com.databricks.spark.csv').save("lv_lat_long2.csv")

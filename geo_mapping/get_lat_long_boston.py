#  to find the latitude and longitude of restaurants
#  and save them with the corresponding classified value 
#  which will be used to plot them on a map in R

from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark import SparkConf, SparkContext
import pylab as pl
import numpy as np
from pyspark.sql.types import *

conf = SparkConf().setAppName("Make lat long for boston")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#read data
lat_long_df = sqlContext\
    .read.json("../boston_data/yelp_academic_dataset_business.json")\
    .select('longitude', 'latitude', 'business_id')

km_df = sqlContext.parquetFile("../models/boston_classified")

#join data
joined_df = lat_long_df.join(km_df, km_df.yelp_id==lat_long_df.business_id)
joined_df = joined_df.select(km_df.risk, lat_long_df.latitude,\
                                 lat_long_df.longitude)

#save as csv
joined_df.repartition(1).write\
    .format('com.databricks.spark.csv').save("lv_lat_boston.csv")

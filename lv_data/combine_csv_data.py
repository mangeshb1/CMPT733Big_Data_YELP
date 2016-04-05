# Combine crowd source csvs with review sentiment vectors 
# Save result as parquet

from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark import SparkConf, SparkContext
import pylab as pl
import numpy as np
from pyspark.sql.types import *

conf = SparkConf().setAppName("READ CSV")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#read csvs

#c1 = rest_df = sqlContext.read.format('com.databricks.spark.csv')\
#     .options(header='true', inferschema='true').load('saif_done.csv')

#c2 = rest_df = sqlContext.read.format('com.databricks.spark.csv')\
#     .options(header='true', inferschema='true').load('mangesh_done.csv')

c1 = rest_df = sqlContext.read.format('com.databricks.spark.csv')\
     .options(header='true', inferschema='true').load('all_done.csv')

#unify csvs
#c1.registerTempTable('c1')

#c2.insertInto('c1')
#c3.insertInto('c1')

#read unify already csv


#read parquet of vectors
data = sqlContext.parquetFile('../models/lv_cluster_long.parquet')

data = data.drop('cluster')

#join 

joined_df = data.join(c1, c1.yelp_id==data.yelp_id)
joined_df = joined_df.drop(c1.yelp_id)

#save as parquet
joined_df.write.parquet("lv_class_input.parquet", mode='overwrite')
print ("COUNT FINAL: ", joined_df.count())

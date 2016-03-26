from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setAppName('Print schema')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df1 = sqlContext.read.json("/user/jna50/yelp/yelp_academic_dataset_business.json")
df2 = sqlContext.read.json("/user/jna50/yelp/yelp_academic_dataset_checkin.json")
df3 = sqlContext.read.json("/user/jna50/yelp/yelp_academic_dataset_review.json")
df4 = sqlContext.read.json("/user/jna50/yelp/yelp_academic_dataset_tip.json")
df5 = sqlContext.read.json("/user/jna50/yelp/yelp_academic_dataset_user.json")

df1.printSchema()
df2.printSchema()
df3.printSchema()
df4.printSchema()
df5.printSchema()


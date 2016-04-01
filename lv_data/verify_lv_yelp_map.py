####  A program that read lv_yelp_map parquet and outputs a csv that can be
####  Used in verification of the map

from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Read lv yelp map and verify")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#read files 
map_loc = "../best_one"
lv_rest = "lv_clean_data.parquet"
yelp_bus = "../yelp_data/yelp_academic_dataset_business.json"

map_df = sqlContext.parquetFile(map_loc)
yelp_df = sqlContext.read.json(yelp_bus)
lv_df = sqlContext.parquetFile(lv_rest)

yelp_df = yelp_df.where(yelp_df.city == "Las Vegas")\
          .select(yelp_df.business_id, yelp_df.full_address,\
                      yelp_df.name)

lv_df = lv_df.select(lv_df.id, lv_df.long_name, lv_df.address)

map_df.registerTempTable("map_df")
m2 = sqlContext.sql("select yelp_id, count(*) as c from map_df  group by yelp_id")
m2 = m2.where(m2.c==1)
m2 = m2.select(m2.yelp_id.alias("yelp_c1"))
#print ("HERE COUNT 0: ", m2.count())

map_small_df = map_df.join(m2, m2.yelp_c1==map_df.yelp_id)
#print ("HERE COUNT 1: ", map_small_df.count())
map_small_df = map_small_df.join(lv_df, lv_df.id==map_small_df.lv_id)
#print ("HERE COUNT 2: ", map_small_df.count())
map_small_df = map_small_df.join(yelp_df, \
                                map_small_df.yelp_id==yelp_df.business_id)
#print ("HERE COUNT: 3", map_small_df.count())

#output as csv
map_small = map_small_df.select(lv_df.id, map_small_df.yelp_id, \
                                lv_df.long_name, yelp_df.name, \
                                lv_df.address, yelp_df.full_address)

#print ("HERE COUNT: ", map_small.count())


map_small = map_small.map(lambda row: row[0] + ";" + row[1] + ";" + row[2] + ";" + \
                     row[3] + ";" + row[4] + ";" + row[5].replace("\n", " "))

map_small = map_small.coalesce(1).cache()

map_small.saveAsTextFile("verification_map.csv")

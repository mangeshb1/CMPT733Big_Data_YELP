### Create a csv file with 2 reviews for each
### restaurant to be used in crowd sourcing
###  

from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *

conf = SparkConf().setAppName("CSV For crowd sourcing")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#read data
lv_yelp_map_loc = "verification_map.csv/fully_verified_121_map"
lv_yelp_map = sqlContext.parquetFile(lv_yelp_map_loc)

lv_df_loc = "../cleaned_data/lv_clean_data.parquet"
lv_df = sqlContext.parquetFile(lv_df_loc)

yelp_df_loc = "../yelp_data/yelp_academic_dataset_review.json"
yelp_df = sqlContext.read.json(yelp_df_loc)

#lv_yelp_map.printSchema()
#lv_df.printSchema()
#yelp_df.printSchema()

def fix_text(s):
    return s.replace('\n', ' ').replace('"', '').replace(',', ' ')\
        .replace(';', '')

#create joining process
joined_df = lv_yelp_map.join(lv_df, lv_df.id==lv_yelp_map.lv_id)\
    .select(lv_yelp_map.lv_id, lv_yelp_map.yelp_id, \
            "short_name", "vio_per_year", "four_scale_stars", \
            "five_scale_stars")

joined_df = joined_df.join(yelp_df, joined_df.yelp_id==yelp_df.business_id)\
    .select(joined_df.lv_id, joined_df.yelp_id, \
            joined_df.short_name.alias("name"), "vio_per_year",\
            "four_scale_stars", "five_scale_stars", \
            yelp_df.text.alias("review_text"), 
            yelp_df.votes.useful.alias("review_usefullness"),\
            yelp_df.review_id)

joined_df = joined_df.dropna(how="any")
joined_df = joined_df.where(joined_df.review_usefullness>0).cache()
#print ("DF COUNT HERE!: ", joined_df.count())

#do i really need this?
joined_df.registerTempTable("joined_df")

yelp_ids = sqlContext.sql("select distinct yelp_id from joined_df").rdd\
    .map(lambda r: r[0]).collect()

revs_short1 = []
revs_short2 = []

#get two reviews for each rest.
for yid in yelp_ids:
    
    this_revs = joined_df.where(joined_df.yelp_id==str(yid))\
        .rdd.map(lambda r: r.review_text).take(2)
    
    if (len(this_revs) == 2):
            revs_short1.append((yid, fix_text(this_revs[0])))
            revs_short2.append((yid, fix_text(this_revs[1])))


revs_df1 = sqlContext.createDataFrame(revs_short1, ["yid1","rev1"])
revs_df2 = sqlContext.createDataFrame(revs_short2, ["yid2","rev2"])

revs_all  = revs_df1.join(revs_df2, revs_df1.yid1==revs_df2.yid2).cache()

joined_df = joined_df.drop('review_text')\
            .drop('review_usefullness')\
            .drop('review_id')

joined_df = joined_df.distinct()

final_out = revs_all.join(joined_df, revs_all.yid1==joined_df.yelp_id)

final_out = final_out.select("yelp_id", 'rev1', 'rev2')

#final_out.write.save("ibm_watson_input", format="parquet")
final_out.repartition(3).write.format('com.databricks.spark.csv')\
            .save('crowd_source3')

final_out.repartition(3).write.format('com.databricks.spark.csv')\
            .save('crowd_source4', delimiter=';')

#final_out.show()

#change final_out to have: (yelp_id, 2 reviews)

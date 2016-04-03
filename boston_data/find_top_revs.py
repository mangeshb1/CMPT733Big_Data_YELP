# Find top ten review for boston rest.
# and save to parquet
# for ibm watson


from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *

conf = SparkConf().setAppName("CSV For IBM Watson")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#read data
yelp_map = sqlContext.read.format('com.databricks.spark.csv')\
     .options(header='true', inferschema='true').load('bos_ton.csv')

yelp_revs = sqlContext.read.json("yelp_academic_dataset_review.json")

#filter revs
yelp_revs = yelp_revs.dropna(how='any').where(yelp_revs.votes.useful>2)

#join

joined_df = yelp_map.join(yelp_revs, \
                          yelp_revs.business_id==yelp_map.yid)

joined_df = joined_df.select(joined_df.yid, \
            "vio_per_year",\
            "four_star", "five_star", \
            yelp_revs.text.alias("review_text"),
            yelp_revs.votes.useful.alias("review_usefullness"),\
            yelp_revs.review_id)

print ("DF COUNT HERE!: ", joined_df.count())

joined_df.registerTempTable("joined_df")
yelp_ids = sqlContext.sql("select distinct yid from joined_df").rdd\
    .map(lambda r: r[0]).collect()

revs_short = []
for yid in yelp_ids:

    this_revs = joined_df.where(joined_df.yid==str(yid))\
        .sort(joined_df.review_usefullness.desc())\
        .rdd.map(lambda r: (r.review_id.encode('ascii','ignore'),)).take(10)

    #print "BLAHBLAH"                                                           
    #print this_revs                                                            
    revs_short.extend(this_revs)

#print revs_short                                                               
revs_df = sqlContext.createDataFrame(revs_short, ["rev_id"])
#revs_df.show()                                                                 

final_out = revs_df.join(joined_df, revs_df.rev_id==joined_df.review_id)
final_out.write.save("ibm_watson_input_boston2", format="parquet")
#final_out.repartition(1).save('lv_ibm_short', 'com.databricks.spark.csv') 

### Create a csv file with the review text for each restaurant
### Limit to top ten reviews only based on usefullness 
### Use the 1 to 1 mapping of lv ids to yelp ids to join the datasets

from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *

conf = SparkConf().setAppName("CSV For IBM Watson")
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
print ("DF COUNT HERE!: ", joined_df.count())

#do i really need this?
joined_df.registerTempTable("joined_df")

#sql_text = "WITH TOPTEN AS (SELECT *, ROW_NUMBER() over (PARTITION BY yelp_id order by review_usefullness desc) AS RowNo FROM joined_df) SELECT * FROM TOPTEN WHERE RowNo <= 10"

#sql_text2 = "SELECT *, ROW_NUMBER() over (PARTITION BY yelp_id order by review_usefullness desc) AS RowNo FROM joined_df"

#sql_text3 = "SELECT rs.Field1, rs.Field2, FROM (SELECT Field1, Field2, RowNumber() over (Partition BY yelp_id ORDER BY 

yelp_ids = sqlContext.sql("select distinct yelp_id from joined_df").rdd\
    .map(lambda r: r[0]).collect()

#print yelp_ids

revs_short = []

for yid in yelp_ids:
    
    this_revs = joined_df.where(joined_df.yelp_id==str(yid))\
        .sort(joined_df.review_usefullness.desc())\
        .rdd.map(lambda r: (r.review_id.encode('ascii','ignore'),)).take(10)

    #print "BLAHBLAH"
    #print this_revs
    revs_short.extend(this_revs)

#print revs_short
revs_df = sqlContext.createDataFrame(revs_short, ["rev_id"])
#revs_df.show()

final_out = revs_df.join(joined_df, revs_df.rev_id==joined_df.review_id)
final_out.write.save("ibm_watson_input", format="parquet")
#final_out.repartition(1).save('lv_ibm_short', 'com.databricks.spark.csv')

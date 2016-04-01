####  A program that can join  yelp_academic_dataset_business.json 
####  with the lv_clean_data.parquet to match the different ids together
####  based on the restaurant name and address
####  Should output parquet or csv that maps yelp business ids to lv 
####  restaurant ids


from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, levenshtein, col
from pyspark import SparkConf, SparkContext
#import pylab as pl
import numpy as np
from pyspark.sql.types import *

def lv_concat(city, addr, zipcode):
    '''Return a concatination of the strings'''

    zip1 = zipcode.split('-')[0]

    rt = addr + " " + city + " NV " + zip1
    return rt

def yelp_addr_fix(addr):
    '''Fix yelp addr:  swap the words, remove newlines, ect'''

    swap_words = swap_words_bc.value

    #remove newlines
    addr = addr.replace("\n", " ")

    #split words
    addr_split = addr.upper().split()
    
    for i in range(len(addr_split)):
        
        w = addr_split[i]

        if w in swap_words:
            addr_split[i] = swap_words[w]

    new_addr = ""
    for w in addr_split:
        new_addr += w + ' '
    new_addr = new_addr[:-1]
    
    return new_addr

conf = SparkConf().setAppName("Join Yelp and LV Data Long Version")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#read files and trim datasets

yelp_loc = "yelp_academic_dataset_business.json"
lv_loc = "lv_clean_data.parquet"

yelp_df = sqlContext.read.json(yelp_loc)
yelp_df.printSchema()
yelp_df = yelp_df.where(yelp_df.city == "Las Vegas")\
          .select(yelp_df.business_id, yelp_df.full_address,\
                      yelp_df.name)

lv_df = sqlContext.parquetFile(lv_loc)
lv_df = lv_df.select(lv_df.id, lv_df.long_name, lv_df.address, \
                         lv_df.zip, lv_df.district)

#read swap words
swap_words = np.genfromtxt("swap_words.csv", skip_header=1,\
                               delimiter=',', dtype=str)
swap_words = dict(swap_words)
swap_words_bc = sc.broadcast(swap_words)

#concat the name addr city and zip
lv_concat_udf = udf(lv_concat, StringType())
lv_df = lv_df.select(lv_df.id.alias("lv_id"), \
                         lv_df.long_name.alias("lv_name"), \
                         lv_concat_udf(lv_df.district,\
                            lv_df.address, lv_df.zip).alias("lv_addr"))

#fix yelp addr
yelp_addr_fix_udf = udf(yelp_addr_fix, StringType())
yelp_df = yelp_df.select(yelp_df.business_id.alias("yelp_id"),\
                  yelp_df.name.alias("yelp_name"),\
                  yelp_addr_fix_udf(yelp_df.full_address).alias("yelp_addr"))

#now the df look like this:
# lv : (id, long_name, lv_addr)
# yelp: (yelp_id, name, yelp_addr)


#joining steps:
# 1 join both lv and yelp df
# 2 find leven. distance on name of restaurant/business
# 3 find min leven distance group by lv id 
# 4 join step 3 with step 2 based on leven distance and lv_id
# 5 repeat steps 2-4 now with leven on address
# 6 remove lv_ids where count is more than 1, since there is a tie in step 5
# 7 save result as parquet with lv_id and yelp_id

#step 1
combined_data = lv_df.join(yelp_df)

#step 2, leven on names
combined_data = combined_data.withColumn("leven_name", 
                levenshtein(col("lv_name"), col("yelp_name")))

#step 3
min_leven  = combined_data.groupby("lv_id").min("leven_name")\
             .select(col("lv_id").alias("lv_id2"), \
             col("min(leven_name)").alias("m_leven_name")) 

combined_data = combined_data.select(combined_data.lv_id, \
                                     combined_data.yelp_id, \
                                     combined_data.lv_addr,\
                                     combined_data.yelp_addr,\
                                   combined_data.leven_name.alias("lev_name"))

#step 4
combined2 = min_leven.join(combined_data,\
                           [min_leven.lv_id2 == combined_data.lv_id, \
                            min_leven.m_leven_name == col("lev_name")])

#step 5

#leven on addr
combined2 = combined2.withColumn("leven_addr",\
                       levenshtein(col("lv_addr"), col("yelp_addr")))

min_leven2  = combined2.groupby("lv_id").min("leven_addr")\
             .select(col("lv_id").alias("lv_id3"), \
             col("min(leven_addr)").alias("m_leven_addr"))

#combine min_leven2 with combined2
combined2 = combined2.select(combined2.lv_id, \
                                     combined2.yelp_id, \
                                     combined2.lv_addr,\
                                     combined2.yelp_addr,\
                                     combined2.lev_name,\
                                   combined2.leven_addr.alias("lev_addr"))

combined3 = min_leven2.join(combined2,\
                           [min_leven2.lv_id3 == combined2.lv_id, \
                            min_leven2.m_leven_addr == col("lev_addr")])

#step 6 add leven_addr with leven_name
add_udf = udf(lambda a, b: a+b, IntegerType())
combined3 = combined3.withColumn("leven_all", add_udf(combined3.lev_addr,\
                                combined3.lev_name))

#find min leven_all
min_leven3  = combined3.groupby("lv_id").min("leven_all")\
             .select(col("lv_id").alias("lv_id4"), \
             col("min(leven_all)").alias("m_leven_all"))

#combine min_leven2 with combined2                                              
combined3 = combined3.select(combined3.lv_id, \
                                     combined3.yelp_id, \
                                     combined3.lv_addr,\
                                     combined3.yelp_addr,\
                                     combined3.lev_name,\
                                     combined3.lev_addr,\
                                   combined3.leven_all.alias("lev_all"))

combined4 = min_leven3.join(combined3,\
                           [min_leven3.lv_id4 == combined3.lv_id, \
                            min_leven3.m_leven_all == col("lev_addr")])

#step 6 not done here

#step 7
combined4 = combined4.select(combined4.lv_id, combined4.yelp_id)
combined4.write.format("parquet").save("lv_to_yelp_map_long.parquet")

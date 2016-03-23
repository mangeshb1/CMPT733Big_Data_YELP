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

def lv_concat(name, city, addr, zipcode):
    '''Return a concatination of the strings'''

    zip1 = zipcode.split('-')[0]

    rt = name + " " + addr + " " + city + " NV " + zip1
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

def yelp_concat(name, addr):
    '''return concat of strings '''
    
    return name + " " + addr

conf = SparkConf().setAppName("Join Yelp and LV Data")
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
lv_df = lv_df.select(lv_df.id, lv_concat_udf(lv_df.long_name, lv_df.district,\
                            lv_df.address, lv_df.zip).alias("lv_full_form"))

#fix yelp addr
yelp_addr_fix_udf = udf(yelp_addr_fix, StringType())
yelp_df = yelp_df.select(yelp_df.business_id.alias("yelp_id"),\
                  yelp_df.name,\
                  yelp_addr_fix_udf(yelp_df.full_address).alias("yelp_addr"))

#yelp concat
yelp_concat_udf = udf(yelp_concat, StringType())
yelp_df = yelp_df.select(yelp_df.yelp_id,\
                         yelp_concat_udf(yelp_df.name, yelp_df.yelp_addr)\
                         .alias("yelp_full_form"))

#now the df look like this:
# lv : (id, lv_full_form)
# yelp: (yelp_id, yelp_full_form)


#joining steps:
# 1 join both lv and yelp df
# 2 find leven. distance
# 3 find min leven distance group by lv id
# 4 join step 3 with step 2 based on leven distance
# 5 remove lv_ids where count is more than 1, since there is a tie in step 3
# 6 save result as parquet with lv_id and yelp_id

#step 1
combined_data = lv_df.join(yelp_df)

#step 2
combined_data = combined_data.select("yelp_id",\
                combined_data.id.alias("lv_id"), \
                levenshtein("lv_full_form", "yelp_full_form").alias("leven1"))

#step 3
min_leven  = combined_data.groupby("lv_id").min("leven1")\
             .select(col("lv_id").alias("lv_id2"), \
                     col("min(leven1)").alias("m_leven"))

#step 4
combined_data = combined_data.select(combined_data.lv_id, \
                                     combined_data.yelp_id, \
                                     combined_data.leven1.alias("lev"))

combined2 = min_leven.join(combined_data,\
                           [min_leven.lv_id2 == combined_data.lv_id, \
                            min_leven.m_leven == combined_data.lev])
#step 5
#not done yet, 

#step 6
combined2 = combined2.select(combined_data.lv_id, combined_data.yelp_id)
combined2.write.format("parquet").save("lv_to_yelp_map_final_d.parquet")

combined2.show()

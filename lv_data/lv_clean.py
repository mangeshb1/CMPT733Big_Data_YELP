### Read CSV FILES                ###
### CREATE NESSECARY DATA FRAMES  ###
### AND PARQUET FILES             ###


file_names = ['restaurant_categories.csv', 'restaurant_cities.csv',\
              'restaurant_establishments.csv', \
              'restaurant_inspection_types.csv', 'restaurants_serials.csv',\
              'restaurant_violations.csv', 'restaurant_pe_category_xref.csv',\
              'restaurant_inspections2.csv']


def fix_address(addr):
    '''Swap words from addr in their full form '''

    swap_words = swap_words_bc.value
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
    

#functions
def diff_dates(begin, end):
    '''Take two dates begin and end and return the difference in years (float)'''
    d1 = begin
    d2 = end

    #print d1
    #print d2

    d1split = d1.split('-')
    d2split = d2.split('-')
    
    y1 = int(d1split[0])
    y2 = int(d2split[0])

    m1 = int(d1split[1])
    m2 = int(d2split[1])

    dy = (y2 + m2/12.0) - (y1 + m1/12.0)
    return dy

def remove_zero(f):
    '''IF f  = 0 return 1'''

    if (f == 0.0):
        #print f
        return 1.0
    return f

def four_scale(v):
    '''Return four scale rank: between 1-4'''

    if (v < 1.456895):
        return 4
    elif ( v < 1.994475):
        return 3
    elif (v < 2.990196):
        return 2
    else:
        return 1

def five_scale(v):
    '''Return five scale rank: between 1-5'''
    
    if (v < 1.354112):
        return 5
    elif (v < 1.749172):
        return 4
    elif (v < 2.184831):
        return 3
    elif (v < 3.434874):
        return 2
    else:
        return 1

from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
from pyspark import SparkConf, SparkContext
import pylab as pl
import numpy as np
from pyspark.sql.types import *

conf = SparkConf().setAppName("READ CSV")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


rest_df = sqlContext.read.format('com.databricks.spark.csv')\
     .options(header='true', inferschema='true').load(file_names[2])

vio_df = sqlContext.read.format('com.databricks.spark.csv')\
     .options(header='true', inferschema='true').load(file_names[-1])

one = udf(lambda v: 1)
vio_df = vio_df.select("RESTAURANT_PERMIT", "DATE", \
                       one(vio_df.GRADE).alias("num"))

vio_df = vio_df.registerTempTable("vio_df")
vio_df = sqlContext.sql("select RESTAURANT_PERMIT, sum(num) as NUM_VIOLATIONS, max(DATE) as fin, min(DATE) as begin from vio_df group by RESTAURANT_PERMIT")

diff_date_udf = udf(lambda d1, d2: diff_dates(d1, d2), FloatType())

vio_df = vio_df.select("RESTAURANT_PERMIT", "NUM_VIOLATIONS",\
        diff_date_udf(vio_df.begin, vio_df.fin).alias("YEAR_DIFF"))

remove_zero_udf = udf(lambda val: remove_zero(val), FloatType())

vio_df = vio_df.select("RESTAURANT_PERMIT", "NUM_VIOLATIONS",\
        remove_zero_udf(vio_df.YEAR_DIFF)\
                       .alias("YEAR_DIFF"))

vio_per_year = udf(lambda vio, yrs: vio / yrs, FloatType())

vio_df = vio_df.select("RESTAURANT_PERMIT", \
         vio_per_year(vio_df.NUM_VIOLATIONS, vio_df.YEAR_DIFF)\
         .alias("VIOLATIONS_PER_YEAR"))


fourscale_udf = udf(four_scale, IntegerType())
fivescale_udf = udf(five_scale, IntegerType())

#assign ranks
vio_df = vio_df.withColumn("four_scale",
                           fourscale_udf(vio_df.VIOLATIONS_PER_YEAR))
vio_df = vio_df.withColumn("five_scale",
                           fivescale_udf(vio_df.VIOLATIONS_PER_YEAR))

#Join with main dataset
all_data = rest_df.join(vio_df, \
                            rest_df.PERMIT_NUMBER == vio_df.RESTAURANT_PERMIT)

#fix addresses with swap words
swap_words = np.genfromtxt("swap_words.csv", skip_header=1,\
                               delimiter=',', dtype=str)
swap_words = dict(swap_words)
swap_words_bc = sc.broadcast(swap_words)

address_fix_udf = udf(fix_address, StringType())

all_data = all_data.withColumn("full_addr", \
                               address_fix_udf(all_data.ADDRESS))

#select needed columns with "nice" alias(es)

all_data = all_data.select(all_data.PERMIT_NUMBER.alias("id"),\
                           all_data.NAME.alias("long_name"), \
                           all_data.LOCATION_NAME.alias("short_name"),\
                           all_data.full_addr.alias("address"),\
                           all_data.LATITUDE.alias("lat"),\
                           all_data.LONGITUDE.alias("long"),\
                           all_data.CITY_NAME.alias("district"),\
                           all_data.ZIP_CODE.alias("zip"),\
                           all_data.CURRENT_GRADE.alias("grade"),\
                           all_data.CURRENT_DEMERITS.alias("demerits"),\
                           all_data.SEARCH_TEXT.alias("search_text"),\
                           all_data.VIOLATIONS_PER_YEAR.alias("vio_per_year"),\
                           all_data.four_scale.alias("four_scale_stars"),\
                           all_data.five_scale.alias("five_scale_stars"))

#save data as parquet file
all_data.write.format("parquet").save("lv_clean_data.parquet")

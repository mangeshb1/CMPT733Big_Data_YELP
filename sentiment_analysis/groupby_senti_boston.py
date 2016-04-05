# Read both the sentiment files (senti) and perform groupby and save parquet

from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import udf, col
import sys

def emotion_extract1(emos):
    '''Return emotion value of anger'''
    s = emos[0]
    return float(s.split(':')[1])

def emotion_extract2(emos):
    '''Return emotion value of disgust'''
    s = emos[1]
    return float(s.split(':')[1])

def emotion_extract3(emos):
    '''Return emotion value of fear'''
    s = emos[2]
    return float(s.split(':')[1])

def emotion_extract4(emos):
    '''Return emotion value of joy'''
    s = emos[3]
    return float(s.split(':')[1])

def emotion_extract5(emos):
    '''Return emotion value of sadness'''
    s = emos[4]
    return float(s.split(':')[1])

def emotion_extract6(emos):
    '''Return emotion value of agreeableness'''

    s = emos[11]
    return float(s.split(':')[1])

def emotion_extract7(emos):
    '''Return emotion value of emotion'''

    s = emos[12]
    return float(s.split(':')[1])

conf = SparkConf().setAppName("IBM Watson Sentimemnt Analysis Group By for boston")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#read boston sentiment
senti_df = sqlContext.parquetFile("boston_senti2")

# remove reviews that have no Sentiment information
# Possibly due to ibm watson errors
sentiment_len_udf = udf(lambda emos: len(emos), IntegerType())
senti_df = senti_df.withColumn("len_emos", \
                                   sentiment_len_udf(senti_df.Sentiment))
senti_df = senti_df.where(senti_df.len_emos==13)
senti_df = senti_df.drop(senti_df.len_emos)

#senti_df.show(1, False)

#perform emos extraction
emotion_extract_udf1 = udf(lambda ls: emotion_extract1(ls), FloatType())
emotion_extract_udf2 = udf(lambda ls: emotion_extract2(ls), FloatType())
emotion_extract_udf3 = udf(lambda ls: emotion_extract3(ls), FloatType())
emotion_extract_udf4 = udf(lambda ls: emotion_extract4(ls), FloatType())
emotion_extract_udf5 = udf(lambda ls: emotion_extract5(ls), FloatType())
emotion_extract_udf6 = udf(lambda ls: emotion_extract6(ls), FloatType())
emotion_extract_udf7 = udf(lambda ls: emotion_extract7(ls), FloatType())

senti_df = senti_df.withColumn("anger",\
                         emotion_extract_udf1(senti_df.Sentiment))
senti_df = senti_df.withColumn("disgust", \
                         emotion_extract_udf2(senti_df.Sentiment))
senti_df = senti_df.withColumn("fear", \
                         emotion_extract_udf3(senti_df.Sentiment))
senti_df = senti_df.withColumn("joy", \
                         emotion_extract_udf4(senti_df.Sentiment))
senti_df = senti_df.withColumn("sadness",\
                         emotion_extract_udf5(senti_df.Sentiment))
senti_df = senti_df.withColumn("agreeness",\
                         emotion_extract_udf6(senti_df.Sentiment))
senti_df = senti_df.withColumn("emotion",\
                         emotion_extract_udf7(senti_df.Sentiment))

#drop unneeded columns
senti_df = senti_df.drop(senti_df.Sentiment).drop(senti_df.rev_id)\
                   .drop(senti_df.review_text).drop(senti_df.review_id)\
                   .drop(senti_df.review_usefullness)

#senti_df.show()

#perform aggregates
senti_agg = senti_df.select(senti_df.yid, "anger", "disgust", "fear", \
                                "joy", "sadness", "agreeness", "emotion")

senti_agg = senti_agg.groupby(senti_df.yid.alias('yid2')).avg().cache()

#join senti_agg with senti_df
senti_df = senti_df.drop('anger').drop('disgust').drop('fear')\
                   .drop('joy').drop('sadness').drop('agreeness')\
                   .drop('emotion')

senti_df = senti_df.distinct()

out_df = senti_df.join(senti_agg, senti_agg.yid2==senti_df.yid)
#print ("TOTAL COUNT: ", out_df.count())

out_df = out_df.select(out_df.yid2, \
                       out_df.vio_per_year, out_df.four_star, \
                       out_df.five_star, \
                       col('avg(anger)').alias('avg_anger'),\
                       col('avg(disgust)').alias('avg_disgust'),\
                       col('avg(fear)').alias('avg_fear'),\
                       col('avg(joy)').alias('avg_joy'),\
                       col('avg(sadness)').alias('avg_sadness'),\
                       col('avg(agreeness)').alias('avg_agree'),\
                       col('avg(emotion)').alias('avg_emotion'))

#save 
out_df.write.parquet("boston_ibm_agg")
out_df.show()

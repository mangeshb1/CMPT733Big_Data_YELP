from pyspark.sql import SQLContext
from watson_developer_cloud import ToneAnalyzerV1Experimental
from watson_developer_cloud import ToneAnalyzerV3Beta
import json,time
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark import SparkContext,SparkConf


sqlContext = SQLContext(sc)

dfIP = sqlContext.read.json("/home/ub51/SFU/733Project/Data/yelp_academic_dataset_review.json").cache()
dfIP.registerTempTable("dfIP")

print dfIP.count()
dfIPFilter = sqlContext.sql("select * from dfIP where votes.useful > 70")
print dfIPFilter.count()


def getSentiment(s):
    tone_analyzer = ToneAnalyzerV3Beta(
    username='1cd1f053-d1a8-44d9-9a1a-04e2c6229eb0',
    password='aj0qwsv4D7uu',
    version='2016-02-11')
    time.sleep(1)
    myvar = tone_analyzer.tone(text=s)
    temp1 = myvar['document_tone']
    temp2 = temp1['tone_categories']

    mylist = []
    for ele in temp2:
        alltones = ele['tones']
        for tone in alltones:
            tone_name = tone['tone_name']
            tone_score = tone['score']
            mylist.append(str(tone_name) + ':' + str(tone_score))
    
    return mylist
        
udfProtToEncoding=udf(lambda s:getSentiment(s), ArrayType(StringType()))

ans = dfIPFilter.withColumn("Sentiment", udfProtToEncoding(dfIPFilter.text))
ans.show(3,False)
#ans.write.parquet("/home/ub51/SFU/733Project/Data/ans.parquet")


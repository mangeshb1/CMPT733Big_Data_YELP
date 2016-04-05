from pyspark.sql import SQLContext
#from watson_developer_cloud import ToneAnalyzerV1Experimental
from watson_developer_cloud import ToneAnalyzerV3Beta
import json,time
import sys
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("IBM Watson Sentimemnt Analysis")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

l = "../boston_data/ibm_watson_input_boston2"
dfIP = sqlContext.parquetFile(sys.argv[1]).cache()
#dfIP_loc = "../lv_data/lv_ibm_short/lv_ibm_input.csv"
#dfIP = sqlContext.read.format('com.databricks.spark.csv')\
#     .options(header='true', inferschema='true').load(dfIP_loc)

dfIP.printSchema()

#no need to filter anymore
#dfIP.registerTempTable("dfIP")
print ("COUNT HERE!!! : ", dfIP.count())
#dfIPFilter = sqlContext.sql("select * from dfIP where votes.useful > 70")
#print dfIPFilter.count()

#dfIP = dfIP.dropna(how='any')

def getSentiment(s):

    if s is None or len(s) == 0:
        #no review_text for some reason
        return [];

    tone_analyzer = ToneAnalyzerV3Beta(
    username='1cd1f053-d1a8-44d9-9a1a-04e2c6229eb0',
    password='aj0qwsv4D7uu',
    version='2016-02-11')
    time.sleep(1)
    try:
        myvar = tone_analyzer.tone(text=s)
    except:
        return [];
    
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

ans = dfIP.withColumn("Sentiment", udfProtToEncoding(dfIP.review_text))
ans.show(3,False)

#print ans.count()
#print ("END COUNT HERE: ", ans.count())

#save 
ans.write.parquet(sys.argv[2])


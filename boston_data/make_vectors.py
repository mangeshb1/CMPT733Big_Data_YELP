# Make vectors for classifier for boston
#
#

from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import udf, col

def col_to_vec(c1,c2,c3,c4,c5,c6,c7,c8,c9):
    ''' return list of c1 to c9 '''
    return [c1, c2, c3, c4, c5, c6, c7, c8, c9]

conf = SparkConf().setAppName("Make boston vectors")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#read boston agg sentiments
bos1 = sqlContext.parquetFile("../sentiment_analysis/boston_ibm_agg")

#read yelp business data to get stars
ydf = sqlContext.read.json("yelp_academic_dataset_review.json")
ydf = ydf.select('business_id', 'stars')


#join
joined = bos1.join(ydf, bos1.yid2==ydf.business_id)

#agg to vect for boston
col_to_vec_udf = udf(lambda c1,c2,c3,c4,c5,c6,c7,c8,c9:\
          [1.0* c1, 1.0 * c2, 1.0 * c3, c4, c5, c6, c7, c8, c9], ArrayType(FloatType()))
joined = joined.withColumn("vector", col_to_vec_udf(joined.stars,\
                                                    joined.four_star,\
                                                    joined.five_star,\
                                                    joined.avg_disgust,\
                                                    joined.avg_agree,\
                                                    joined.avg_emotion,\
                                                    joined.avg_sadness,\
                                                    joined.avg_anger,\
                                                    joined.avg_joy))

joined = joined.select(joined.yid2.alias('yelp_id'), joined.vector)

#save 
joined.write.parquet("boston_vectors", mode='overwrite')

joined.show()

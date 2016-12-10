from pyspark import SparkConf,SQLContext
import pyspark_cassandra
import sys,re
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils

def stem(words):
    from nltk.stem.porter import PorterStemmer
    steming = PorterStemmer()
    return [steming.stem(word.decode('utf-8')) for word in words]

def returnSentiment(star):
    digit = int(round(star,1))
    if (star - digit) >0.5:
       return (digit+1)
    return digit   

def cleanText(review):
    import nltk,os 
    nltk.data.path.append('/home/mchopra/nltk_data')
   
    #remove Punctuation #remove Digits  
    review=re.sub("[^a-zA-Z]", " ", review)
    
    #change to lower case   
    review=review.lower()
    #split words
    words = review.split(' ')                             
    
    # a list, so convert the stop words to a set
    from nltk.corpus import stopwords
    stops = set(stopwords.words("english"))                  
     
    #Remove stop words
    meaningful_words = [w for w in words if not w in stops]   
    

    #do steming
    stemmed_words=meaningful_words

    #join back review
    review = " ".join( stemmed_words )  
    
    review.encode('utf-8','ignore')

    return review 

    
def main(keyspace,sc,sqlContext,output_path):
    table1= "business"
    table2= "review"

    rdd=sc.cassandraTable(keyspace, table1,
                          row_format=pyspark_cassandra.RowFormat.DICT).select('business_id','categories')
    rdd=rdd.filter(lambda row: ('Restaurants' in row['categories']))
    sqlContext.createDataFrame(rdd).registerTempTable(table1)
    
    rdd=sc.cassandraTable(keyspace, table2,
                          row_format=pyspark_cassandra.RowFormat.DICT).select('business_id','user_id','text','stars')
    sqlContext.createDataFrame(rdd).registerTempTable(table2)


      
    
    result=sqlContext.sql("""
                    SELECT r.business_id,r.user_id,r.text,r.stars
                    FROM review r
                    JOIN business b ON (b.business_id =r.business_id)
                    """)

    rdd  = result.rdd.map(lambda row: (row['business_id'],row['user_id'],cleanText(row['text']),returnSentiment(row['stars']))) 
    textData=sqlContext.createDataFrame(rdd,['business_id','user_id','text','stars'])

    #TFID to give value to form vector corresponding to each sentence 
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    wordsData = tokenizer.transform(textData)
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=100)
    featurizedData = hashingTF.transform(wordsData)
    featurizedData.save(output_path+'/TokenizedWords')
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)
    rdd = rescaledData.select("features", "stars").rdd
    #convertToLabeledPointRDD 
    rdd = rdd.map(lambda row:LabeledPoint(row['stars'],row['features']))
    training, test = rdd.randomSplit([0.8, 0.2])
    #MLUtils.saveAsLibSVMFile(training,(output_path+'/data_training'))
    training.saveAsTextFile(output_path+'/data_training')
    #MLUtils.saveAsLibSVMFile(test,(output_path+'/data_test'))
    test.saveAsTextFile(output_path+'/data_test')
    
    

     
if __name__ == "__main__":
    
    keyspace_input=sys.argv[1]
    output_path=sys.argv[2]
    cluster_seeds = ['199.60.17.136', '199.60.17.173']
    
    conf = SparkConf().set('spark.cassandra.connection.host',
        ','.join(cluster_seeds)).set('spark.dynamicAllocation.maxExecutors', 20)
    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    main(keyspace_input,sc,sqlContext,output_path)

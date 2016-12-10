from pyspark import SparkConf,SQLContext,SparkContext
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.ml.feature import HashingTF, IDF,StopWordsRemover
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
from Levenshtein import distance
import pyspark_cassandra
import sys,re,itertools,math
def calculateRMSE(model,data):
    
    data1 = data.map(lambda (u,m,r):(u,m))   
    predictions = model.predictAll(data1).map(lambda (u,m,r): ((u,m), float(r)))
    ratesAndPreds = data.map(lambda (u,m,r): ((u,m), float(r))).join(predictions)
    value=ratesAndPreds.map(lambda (k,(r1,r2)): ((r1 - r2)**2))
    meanValue=value.mean()
    RMSE = math.sqrt(meanValue)
    return RMSE

def returnStar(star):
    digit = int(round(star,1))
    if (star - digit) >0.5:
       return (digit+1)
    return float(digit)   

def parseLine(line,business,business_id):
    splitLine=line.split('#')
    review= splitLine[0]
    #review=re.sub("[^a-zA-Z]", " ", review)
    #review=review.lower()
    review= review.split(" ") 
    restrauntName =(splitLine[1]).strip()
    restrauntid=''
    minDistance = 10000
    for k,v in business.items():    
        dist = distance(v,restrauntName) 
        if dist < minDistance:
            restrauntid=k
            minDistance = dist
    #assign userId as 0
    return (0,business_id[restrauntid],review)

def parseUserRatings(row,users,business):
    user_id=users[row['user_id']]
    business_id=business[row['business_id']]
    rating=returnStar(row['stars'])

    return (user_id,business_id,rating) 

def  main(inputs,city,day,output,sc):
	#Read User Tweets
     sqlContext = SQLContext(sc)
     tweets = sc.textFile(inputs+'/user_tweet.txt')
     UserMap =dict()
     BusinessMap =dict()
     business =sqlContext.parquetFile(inputs+'/restaurant_data')

     business_rdd=business.rdd.map(lambda row:(row['business_id'],row['name']))
     business_collection = dict(business_rdd.collect())
     BusinessIdCollection=business.rdd.map(lambda row:row['business_id']).collect()
     i=1
     for id in  BusinessIdCollection:
        BusinessMap[id]=i
        i=i+1

     broadcaste_business=sc.broadcast(business_collection)
     broadcaste_business_id=sc.broadcast(BusinessMap) 
     

     tweets =tweets.map(lambda line:parseLine(line,broadcaste_business.value,broadcaste_business_id.value))
     
     
     user = sqlContext.parquetFile(inputs+'/reviewer_data')
     userIdCollection=user.rdd.map(lambda row:row['user_id']).distinct().collect()
     
     i=1
     for id in  userIdCollection:
        UserMap[id]=i
        i=i+1
     broadcaste_user_id =sc.broadcast(UserMap) 
     ratings = user.rdd.map(lambda row:parseUserRatings(row,broadcaste_user_id.value,broadcaste_business_id.value))
     #cache ratings since going to be used in collaborative filtering  
     ratings.cache()
     df_tweets=sqlContext.createDataFrame(tweets,["user_id","business_id","review"])
     #remove stopwords
     remover = StopWordsRemover(inputCol="review", outputCol="words")
     filtered_review=remover.transform(df_tweets)
     #apply hashing
     hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=100)
     featurizedData = hashingTF.transform(filtered_review)
     #transform the features
     idf = IDF(inputCol="rawFeatures", outputCol="filtered_review")
     idfModel = idf.fit(featurizedData)
     rescaledData = idfModel.transform(featurizedData)
     rdd=rescaledData.rdd
     #load the model
     model = NaiveBayesModel.load(sc, inputs+'/rating_predictor')
     #predicted_User_Rating
     userRating = rdd.map(lambda row: (row['user_id'],row['business_id'],model.predict(row['filtered_review'])))
     userRating.saveAsTextFile(output + '/Recommendation/predictedRatingUser')     
     userRating.cache() 
    
    #cache all the training,test and validation since we are going to use them multiple times    
     
     [training,validation,test] = ratings.union(userRating).randomSplit([0.6, 0.2,0.2])
     training.cache()
     validation.cache()
     test.cache()

    

     numTraining = training.count()
     numValidation = validation.count()
     numTest = test.count()
     information_list =list()
    
     str='no of training Data = %d'%(numTraining)
     information_list.append(str)
     str='no of validation Data = %d'%(numValidation)
     information_list.append(str)
     str='no of Test Data = %d'%(numTest)
     information_list.append(str)

     ranks=[8]
     lambdas =[0.1]
     iterations=[10]
     best_Model = None
     best_RMSE = float("inf")
     best_Rank = -1
     best_lambda = -1
     best_NumIter = -1
      
     for rank,lmbda,itr in itertools.product(ranks,lambdas,iterations):
        train_model=ALS.train(training,rank,itr,lmbda)
        newvalRMSE =calculateRMSE(train_model,validation)
        if newvalRMSE < best_RMSE:
            best_Model = train_model
            best_lambda = lmbda
            best_Rank = rank
            best_RMSE = newvalRMSE
            best_NumIter = itr

     testRMSE = calculateRMSE(best_Model,test)

     str='Best Model has following features:- \
        \nlambda = %f,\nRank = %d,\nIterations= %d,\nRMSE on Test=%f'%(best_lambda,best_Rank,best_NumIter,testRMSE)
     information_list.append(str)

    
     information_rdd=sc.parallelize(information_list)
     recommendedList=best_Model.recommendProducts(0,2000)

     information_rdd=sc.parallelize(information_list)
     recommendation_rdd=sc.parallelize(recommendedList)
    #since information of model is better to be seen at one place 
     (information_rdd.coalesce(1).saveAsTextFile(output+'/Recommendation/Model_Information'))
    #since only top 50 shown
    #reverse the list
     temp_dict=BusinessMap
     temp_rdd =sc.parallelize(temp_dict.items()).map(lambda (k,v):(v,k))
     temp_dict=dict(temp_rdd.collect())
     broadcast_revDict=sc.broadcast(temp_dict)
     def parseBusinessId(row,dictionary,business):
        business_id=dictionary[row[1]]
        business_name=business[business_id]
        return (business_id,business_name,row[2])

         

     recommendation_rdd=recommendation_rdd.map(lambda row:parseBusinessId(row,broadcast_revDict.value,broadcaste_business.value))


     (recommendation_rdd.coalesce(1)
                    .saveAsTextFile(output+'/Recommendation/Recommendation_List'))
     

     sqlContext.createDataFrame(recommendation_rdd,['business_id','name','rating']).registerTempTable('actual_recommend')
     df =sqlContext.parquetFile(inputs+'/city/'+city)
     df.where(df.day == day).registerTempTable('checkin_info')
     sqlContext.parquetFile(inputs+'/good_bad_reviews').registerTempTable('user_review')
     result=sqlContext.sql("""
                    SELECT ar.name,ch.count,ur.good_count,ur.bad_count
                    FROM actual_recommend ar
                    JOIN checkin_info ch ON (ar.business_id = ch.business_id)
                    JOIN user_review  ur ON (ch.business_id = ur.business_id)
                    """)
     rdd=result.rdd.map(lambda row:(row['name'],row['count'],row['good_count'],row['bad_count'])).sortBy(lambda (n,c,g,b): (-c,-g,b))
     #rdd =rdd.map(lambda (n,c,g,b): ('name ='+n+', no of checkin ='+str(c)+',  no of good review ='+str(g) + ', no of bad review ='+str(b)))
     result=sqlContext.createDataFrame(rdd,['Restaurant name','No of Checkin','Good Reviews','Bad Reviews'])
     result.show()
     #rdd.saveAsTextFile(output+'/Final_Answer') 









if __name__ == "__main__":
    inputs=sys.argv[1]
    city=(sys.argv[2]).replace(" ","").lower()
    day=int(sys.argv[3])
    output=sys.argv[4]
    
    cluster_seeds = ['199.60.17.136', '199.60.17.173']
    
    conf = SparkConf().set('spark.cassandra.connection.host',
        ','.join(cluster_seeds)).set('spark.dynamicAllocation.maxExecutors', 20)
    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
    
    main(inputs,city,day,output,sc)


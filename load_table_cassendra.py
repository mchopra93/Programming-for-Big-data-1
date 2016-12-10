from pyspark import SparkConf, SparkContext
import pyspark_cassandra
import sys,json,re,uuid,datetime


def returnRDD(input,file,keyspace,sc):
    input_text =sc.textFile(inputs).repartition(sc.defaultParallelism * 20)
    input_RDD=input_text.map(json.loads)     
    return  input_RDD    

    



 
def main(inputs,keyspace,sc):
    
    input=inputs+'/yelp_academic_dataset_business.json'
    Business_rdd=returnRDD(input,file,keyspace,sc)      
    Business_rdd=Business_rdd.filter(lambda object: object['type'] =='business' ).map(lambda row:
                                                                                        {
                                                                       "business_id":row["business_id"],
                                                                       "categories":','.join(row["categories"]),
                                                                       "city":row["city"],
                                                                       "full_address":row["full_address"],
                                                                       "latitude":str(row["latitude"]),
                                                                       "longitude":str(row["longitude"]),
                                                                       "name":row["name"],
                                                                       "review_count":int(row["review_count"]),
                                                                       "stars":float(row["stars"]),
                                                                       "state":row["state"]
                                                                        })


     
    Business_rdd.saveToCassandra(keyspace,'business')
    
    input=inputs+'/yelp_academic_dataset_review.json'
    Review_rdd=returnRDD(input,file,keyspace,sc)
    Review_rdd=Review_rdd.filter(lambda object: object['type'] =='review').map(lambda row:{
                                                                           "business_id":row["business_id"],
                                                                           "user_id":row["user_id"],
                                                                           "date":datetime.datetime.strptime(row["date"],'%Y-%m-%d'),
                                                                           "stars":float(row["stars"]),
                                                                           "text":str((row["text"]).encode('utf-8', 'ignore')).replace('\n','')
                                                                           } )



    Review_rdd.saveToCassandra(keyspace,'review')
    input=inputs+'/yelp_academic_dataset_checkin.json'
    Checkin_rdd=returnRDD(input,file,keyspace,sc)
    
    
    Checkin_rdd=Checkin_rdd.filter(lambda object: object['type'] =='checkin').map(lambda row:{
                                                                           "business_id":row["business_id"],
                                                                           "checkin_info":str(row["checkin_info"]) 
                                                                           } )



    Checkin_rdd.saveToCassandra(keyspace,'checkin')
    input=inputs+'/yelp_academic_dataset_tip.json'
    Tip_rdd=returnRDD(input,file,keyspace,sc)
    
   

    Tip_rdd=Tip_rdd.filter(lambda object: object['type'] =='tip').map(lambda row:{
                                                                                  'text': str((row["text"]).encode('utf-8', 'ignore')).strip().replace('\n',''),
                                                                                  'business_id': row['business_id'],
                                                                                  'user_id': row['user_id'],
                                                                                  'date': datetime.datetime.strptime(row["date"],'%Y-%m-%d'),
                                                                                  'likes':int(row['likes'])
                                                                                   } )



    Tip_rdd.saveToCassandra(keyspace,'tip')
    
    
if __name__ == "__main__":
    inputs=sys.argv[1]
    keyspace=sys.argv[2]
    
    cluster_seeds = ['199.60.17.136', '199.60.17.173']

    conf = SparkConf().set('spark.cassandra.connection.host',
        ','.join(cluster_seeds))
    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
    main(inputs,keyspace,sc)








    
        	



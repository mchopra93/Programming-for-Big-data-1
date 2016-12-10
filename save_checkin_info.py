from pyspark import SparkConf
import pyspark_cassandra
import sys
from pyspark.sql import SQLContext

def returnSentiment(star):
    if(star >= 3):
        return 'good';
    else:
        return 'bad';


def parseCheckInfo(row):
    business_id = row['business_id']
    city =row['city']
    list_obj = row['checkin_info']
    lists=list()
    for value in list_obj.replace("}","").split(','):
        value=value.split("-")[1]
        day = int(value.split("':")[0])
        count = int(value.split("':")[1])
        lists.append(((business_id,city,day),count))
    return (lists) 

def main(keyspace,sc,sqlContext,output_path):
    table1= "business"
    table2= "review"
    table3="checkin"

    rdd=sc.cassandraTable(keyspace, table1,
                         row_format=pyspark_cassandra.RowFormat.DICT).select('business_id','categories','city')
    rdd=rdd.filter(lambda row: 'Restaurants' in row['categories'] )
    sqlContext.createDataFrame(rdd).registerTempTable(table1)
    df = (sqlContext.sql("""SELECT city from business"""))
    city_list=df.rdd.map(lambda line: (line['city']).replace(" ","").lower()).distinct().collect()
    
    rdd=sc.cassandraTable(keyspace, table2,
                          row_format=pyspark_cassandra.RowFormat.DICT).select('business_id','stars')
    sqlContext.createDataFrame(rdd).registerTempTable(table2)

    
    result=sqlContext.sql("""
                    SELECT r.business_id,r.stars
                    FROM review r
                    JOIN business b ON (b.business_id =r.business_id)
                    """)
    rdd  = result.rdd.map(lambda row: (row['business_id'],returnSentiment(row['stars']))) 
    goodrdd = rdd.filter(lambda row:row[1] == 'good').map(lambda row:(row[0],1)).reduceByKey(lambda x,y:x+y)
    badrdd = rdd.filter(lambda row:row[1] == 'bad').map(lambda row:(row[0],1)).reduceByKey(lambda x,y:x+y)
    joined_rdd=(goodrdd.join(badrdd)).map(lambda row:(row[0],row[1][0],row[1][1]))
    df = sqlContext.createDataFrame(joined_rdd,['business_id','good_count','bad_count'])

    df.write.parquet(output_path+'/good_bad_reviews')
    
    
    checkinTable=sc.cassandraTable(keyspace, table3,
                          row_format=pyspark_cassandra.RowFormat.DICT)
    sqlContext.createDataFrame(checkinTable).registerTempTable(table3)


    checkinfoquery=sqlContext.sql("""
                        SELECT c.business_id,b.city,c.checkin_info
                        FROM  checkin c
                        JOIN  business b ON (b.business_id =c.business_id)
                        """).rdd
    

    checkinfoquery= checkinfoquery.flatMap(parseCheckInfo).reduceByKey(lambda x,y: x+y).sortBy(lambda (key,value):-value)
    checkinfoquery.cache()  
    for city in city_list:
        checkinrdd=checkinfoquery.filter(lambda row:(row[0][1]).replace(" ","").lower() == city).map(lambda ((b,s,d),c): (b,d,c))
        if checkinrdd.count() > 0:
            df =sqlContext.createDataFrame(checkinrdd,['business_id','day','count'])
            df.write.parquet(output_path+'/city/'+city)   
        
   
     
if __name__ == "__main__":
    
    keyspace_input=sys.argv[1]
    output_path=sys.argv[2]
    cluster_seeds = ['199.60.17.136', '199.60.17.173']
    
    conf = SparkConf().set('spark.cassandra.connection.host',
        ','.join(cluster_seeds)).set('spark.dynamicAllocation.maxExecutors', 20)
    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    main(keyspace_input,sc,sqlContext,output_path)
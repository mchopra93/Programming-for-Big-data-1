from pyspark import SparkConf,SQLContext
import pyspark_cassandra
import sys,re,math,itertools

    
def main(keyspace,sc,sqlContext,output_path):
    
    table1= "business"
    table2= "review"
    
    rdd=sc.cassandraTable(keyspace, table1,
                          row_format=pyspark_cassandra.RowFormat.DICT).select('business_id','name','categories')
    rdd=rdd.filter(lambda row: ('Restaurants' in row['categories']) )

    
    df_business=sqlContext.createDataFrame(rdd)
    df_business.registerTempTable(table1)
    
    df_business.select('business_id','name').write.parquet(output_path+"/restaurant_data")

    rdd=sc.cassandraTable(keyspace, table2,
                          row_format=pyspark_cassandra.RowFormat.DICT).select('business_id','user_id','text','stars')
    sqlContext.createDataFrame(rdd).registerTempTable(table2)

     
      
    
    result=sqlContext.sql("""
                    SELECT r.business_id,r.user_id,r.text,r.stars
                    FROM review r
                    JOIN business b ON (b.business_id =r.business_id)
                    """)
    
    result.write.parquet(output_path+"/reviewer_data")
    

     
if __name__ == "__main__":
    
    keyspace_input=sys.argv[1]
    output_path=sys.argv[2]
    cluster_seeds = ['199.60.17.136', '199.60.17.173']
    
    conf = SparkConf().set('spark.cassandra.connection.host',
        ','.join(cluster_seeds)).set('spark.dynamicAllocation.maxExecutors', 20)
    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    main(keyspace_input,sc,sqlContext,output_path)

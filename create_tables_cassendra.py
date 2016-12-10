from cassandra.cluster import Cluster
import sys,os,re

keyspace=sys.argv[1]

cluster = Cluster(['199.60.17.136', '199.60.17.173'])
session = cluster.connect(keyspace)


#create the table if does not exist
print "CREATING TABLE BUSINESS..."
session.execute("CREATE TABLE if not exists Business  ( business_id TEXT PRIMARY KEY,name TEXT,city TEXT,state TEXT,full_address TEXT,latitude TEXT,longitude TEXT,stars decimal,review_count INT,categories TEXT)")
print "CREATING TABLE REVIEW..."
session.execute("CREATE TABLE if not exists Review (business_id TEXT,user_id TEXT,stars decimal,text TEXT,date TIMESTAMP,PRIMARY KEY(business_id,user_id))")
print "CREATING TABLE USER..."
session.execute("CREATE TABLE if not exists User (user_id TEXT PRIMARY KEY,name TEXT,review_count INT,average_stars decimal,yelping_since TIMESTAMP,fans INT) ")
print "CREATING TABLE CHECKIN..."
session.execute("CREATE TABLE if not exists Checkin (business_id TEXT,Checkin_info TEXT,PRIMARY KEY(business_id)) ")
print "CREATING TABLE Tip..."
session.execute("CREATE TABLE if not exists Tip (business_id TEXT,user_id TEXT,text TEXT,likes int,date TIMESTAMP,PRIMARY KEY(business_id,user_id))")
print "SUCCESSFULLY CREATED ALL THE TABLES"








    
        	



# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 09:41:11 2021

@author: Administrator

For No headrer in file

"""
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name = str(fields[1].encode("utf-8")), age = int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache() #this is for optimization
schemaPeople.createOrReplaceTempView("people")

#Put SQL Here
#teenagers = spark.sql("SELECT * FROM people WHERE age>= 13 AND age <=19")
#elders = spark.sql("SELECT name FROM people WHERE age>= 40")
#unpopulars = spark.sql("SELECT * FROM people WHERE numFriends <=50")
#populars = spark.sql("SELECT * FROM people WHERE numFriends >= 300")
populars = spark.sql("SELECT name, numFriends FROM people ORDER BY numFriends DESC LIMIT 5")

#collect data
for popular in populars.collect():
    print(popular)
    
#or
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
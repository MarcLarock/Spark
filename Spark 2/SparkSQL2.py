# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 08:58:43 2021

@author: Administrator
"""

from pyspark.sql import SparkSession

#create spark session
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

friends = spark.read.option("header","true").option("inferSchema","true")\
    .csv("fakefriends-header.csv")

print("infer schema")
friends.printSchema()

friends.select("name").show()

friends.filter(friends.age<21).show()

friends.groupBy("age").count().show()

spark.stop()
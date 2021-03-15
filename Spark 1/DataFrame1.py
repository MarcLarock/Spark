# -*- coding: utf-8 -*-
"""
Created on Tue Mar  9 14:11:55 2021

@author: Administrator

find the top 5 countries for generating the most revenue
"""


from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

#create the Spark session
spark = SparkSession.builder.appName("DataFrame").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema","true").csv("10000_Sales_Records.csv")

#select the right columns from the data frame
region = df.select("Country","Total Revenue")

#do summation
total = region.groupBy("Country").sum("Total Revenue")

#sort and then print
totalSales = total.sort(func.desc("sum(Total Revenue)"))

#display results
totalSales.show(5)

#stop the spark session
spark.stop()
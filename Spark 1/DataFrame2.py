
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("Question 7").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema","true").csv("10000_Sales_Records.csv")
#slect columns
region = df.select("Region","Total Profit")
#summation
total = region.groupBy("Region").sum("Total Profit")
#sort and then print
totalProfit = total.sort(func.desc("sum(Total Profit)"))

totalProfit.show(20)

spark.stop()
//find the top 5 countries for generating the most revenue
package scalaspark

import org.apache.spark._
import org.apache.log4j._
import scala.math._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.{functions => func}
import org.apache.spark.sql.types.{IntegerType, StringType, FloatType, StructType}

object DataSet {
    
    case class Sales(region: String, country: String, itemType: String, salesChannel: String, 
                        orderPriority: String, orderDate: String, orderID: Int, shipDate: String, unitsSold: Int,
                        unitPrice: Float, unitCost: Float, totalRevenue: Float, totalCost: Float, totalProfit: Float)
    
    def main(args: Array[String]) {        

        Logger.getLogger("org").setLevel(Level.ERROR)
    
        val spark = SparkSession.builder
            .appName("Q1")
            .master("local[*]")
            .getOrCreate()
        
        val salesSchema = new StructType()
            .add("region", StringType, nullable = true)
            .add("country", StringType, nullable = true)
            .add("itemType", StringType, nullable = true)
            .add("salesChannel", StringType, nullable = true)
            .add("orderPriority", StringType, nullable = true)
            .add("orderDate", StringType, nullable = true)
            .add("orderID", IntegerType, nullable = true)
            .add("shipDate", StringType, nullable = true)
            .add("unitsSold", IntegerType, nullable = true)
            .add("unitPrice", FloatType, nullable = true)
            .add("unitCost", FloatType, nullable = true)
            .add("totalRevenue", FloatType, nullable = true)
            .add("totalCost", FloatType, nullable = true)
            .add("totalProfit", FloatType, nullable = true)
        
        import spark.implicits._
        
        val ds = spark.read
            .option("header", "true")
            .schema(salesSchema)            
            .csv("data/10000_Sales_Records.csv")
            .as[Sales]
        
    //use groupBy to grab country column from the data set    
    val country = ds.groupBy("country").sum("totalRevenue")
    
    //Sum over the total profits and present in descending order
    val sorted = country.orderBy(col("sum(totalRevenue)").desc)
    
    //display the top 5
    sorted.show(5)
    
    spark.stop()
    
         
  }   
}
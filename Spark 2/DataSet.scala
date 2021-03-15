package scalaspark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object fakes {
  
  case class Friend(userID: Int, name:String, age:Int, numbFriends: Int)
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder
    .appName("Friends")
    .master("local[*]")
    .getOrCreate()
    
    val friendsSchema = new StructType()
     .add("userID", IntegerType, nullable=true)
     .add("name", StringType, nullable=true)
     .add("age", IntegerType, nullable=true)
     .add("numbFriends", IntegerType, nullable =true)
     
     import spark.implicits._
     val friendsDS = spark.read.schema(friendsSchema)
         .csv("data/fakefriends.csv")
         .as[Friend]
    
    
    val friends = friendsDS.select("age","numbFriends")
    
    val results = friends.groupBy("age").avg("numbFriends") 
    
    val sortedResults = results.sort("age")
    
    sortedResults.show()

    
  }
  
}

package casestudy.rohit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.collection.immutable.List
import org.apache.spark.SparkConf
import scala.io.Source

object Top_5_Vehicle_08 extends App {        
  
   Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "append column")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
  
  val chargesDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "E:/BCG/Data/Charges_use.csv")
    .load()
    
    val unitDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "E:/BCG/Data/Units_use.csv")
    .load()
    
    val primaryDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "E:/BCG/Data/Primary_Person_use.csv")
    .load()
    
    val filterSpeedDF = chargesDf.where(col("CHARGE").like("%PEED%"))
    
 
  
  val topColorDF = unitDf.withColumn("count", count("VEH_COLOR_ID").over(Window.partitionBy("VEH_COLOR_ID").orderBy(col("VEH_COLOR_ID").desc)))
  
  val top10ColorDF = topColorDF.withColumn("rank",dense_rank().over(Window.orderBy(col("count").desc))).where(col("rank") <= 10)
  .drop("count").drop("rank")
  
  val topStateDf = primaryDf.withColumn("count", count("DRVR_LIC_STATE_ID").over(Window.partitionBy("DRVR_LIC_STATE_ID")))
  
  val top25StateDf = topStateDf.withColumn("rank",dense_rank().over(Window.orderBy(col("count").desc))).where(col("rank") > 25 && col("DRVR_LIC_TYPE_ID") === "DRIVER LICENSE")
 .drop("count").drop("rank")
 
  val JoinedDF = top10ColorDF.join(top25StateDf,top10ColorDF("CRASH_ID") === top25StateDf("CRASH_ID"),"inner" )
  .join(filterSpeedDF,top10ColorDF("CRASH_ID") === filterSpeedDF("CRASH_ID"),"inner")
  .drop(top25StateDf.col("CRASH_ID"))
  .drop(filterSpeedDF.col("CRASH_ID"))
  
val topMakeDf = JoinedDF.withColumn("count", count("VEH_MAKE_ID").over(Window.partitionBy("VEH_MAKE_ID")))
  
  val top5MakeDf = topMakeDf.withColumn("rank",dense_rank().over(Window.orderBy(col("count").desc))).where(col("rank") <= 5)
 .drop("count").drop("rank")
 
top5MakeDf.select(col("VEH_MAKE_ID")).distinct().show()
 
 
  
  

                     
             
                      

}
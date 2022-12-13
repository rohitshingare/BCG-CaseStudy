package casestudy.rohit

import org.apache.spark.SparkConf
import org.apache.spark._
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

object Largest_Number_Of_Injuries extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "highest number of accidents in which females are involved")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val readerDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "E:/BCG/Data/Units_use.csv")
    .load()
    

     
val df1 =  readerDf.groupBy("VEH_MAKE_ID").agg(sum("TOT_INJRY_CNT").as("total")).sort(col("total").desc)
      
val myWindow = Window.partitionBy(df1.col("VEH_MAKE_ID")).orderBy(df1.col("total").desc)

df1.withColumn("row_number", rank.over(myWindow)).show()
 /* val df1 = readerDf.withColumn("TOT_INJRY_CNT", sum("TOT_INJRY_CNT").over(myWindow))
  
  val df2 = df1.withColumn("dense_rank",dense_rank().over(myWindow)).show*/
  
  
}
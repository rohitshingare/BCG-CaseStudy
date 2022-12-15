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

object Highest_Number_Of_Accident_Female_Envolved extends App {
//Analysis 3: Which state has highest number of accidents in which females are involved?  
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
    .option("path", "E:/BCG/Data/Primary_Person_use.csv")
    .load()

  val countDf = readerDf.where(!col("DRVR_LIC_STATE_ID").isin("NA", "Unknown"))
    .groupBy("DRVR_LIC_STATE_ID", "PRSN_GNDR_ID")
    .agg(count("*").as("total_count"))

  val sortedDf = countDf
    .where(col("PRSN_GNDR_ID") === "FEMALE")
    .sort(col("total_count").desc)
    .drop("PRSN_GNDR_ID")
    .drop("total_count")
    .first()

  //print(sortedDf.first())

  spark.close()

}
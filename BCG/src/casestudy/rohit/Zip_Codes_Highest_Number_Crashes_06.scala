package casestudy.rohit
import org.apache.spark.SparkConf
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

object Zip_Codes_Highest_Number_Crashes extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "Top 5 Zip Codes with highest number crashes with alcohols")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val primaryPersonDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "E:/BCG/Data/Primary_Person_use.csv")
    .load()

  val unitDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "E:/BCG/Data/Units_use.csv")
    .load()

  val joinCondition = primaryPersonDf.col("CRASH_ID") === unitDf.col("CRASH_ID")

  val joinType = "inner"

  val JoinedDf = unitDf.join(broadcast(primaryPersonDf), joinCondition, joinType).drop(unitDf.col("CRASH_ID"))

  val carCrashesDF = JoinedDf.where(col("VEH_BODY_STYL_ID").isin("PASSENGER CAR, 4-DOOR", "PASSENGER CAR, 2-DOOR", "POLICE CAR/TRUCK"))

  val topDF = carCrashesDF.where(col("PRSN_ALC_RSLT_ID") === "Positive" && col("DRVR_ZIP").isNotNull)
    .groupBy(col("DRVR_ZIP"))
    .agg(count("*").as("total_crashes"))
    .sort(col("total_crashes").desc)

  val top5DF = topDF.withColumn("row_number", row_number().over(Window.orderBy(col("total_crashes").desc)))
    .where(col("row_number") <= 5).drop("row_number").drop("total_crashes").show

  spark.close()

}
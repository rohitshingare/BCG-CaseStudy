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

object Two_Wheeler_Booked_For_Crashes extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "total number of two wheelers are booked for crashes")
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

  val notApplicableDF = primaryPersonDf.where(col("PRSN_HELMET_ID") !== "NOT APPLICABLE")

  val motorDF = unitDf.where(col("VEH_BODY_STYL_ID").isin("MOTORCYCLE", "POLICE MOTORCYCLE"))

  val joinCondition = motorDF.col("CRASH_ID") === notApplicableDF.col("CRASH_ID")

  val joinType = "inner"

  val JoinedDf = unitDf.join(primaryPersonDf, joinCondition, joinType).drop(unitDf.col("CRASH_ID"))

  println("total of two wheelers are booked for crashes : " + JoinedDf.count())

  spark.close()
}
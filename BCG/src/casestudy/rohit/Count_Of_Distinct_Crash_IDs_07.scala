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
import org.apache.spark.sql.functions._
import scala.collection.immutable.List

object Count_Of_Distinct_Crash_IDs extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "append column")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val unitDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "E:/BCG/Data/Units_use.csv")
    .load()

  val damagesDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "E:/BCG/Data/Damages_use.csv")
    .load()

  val joinCondition = unitDf.col("CRASH_ID") === damagesDf.col("CRASH_ID")

  val joinType = "inner"

  val JoinedDf = unitDf.join(broadcast(damagesDf), joinCondition, joinType).drop(unitDf.col("CRASH_ID"))
  
  val noDamageDF = JoinedDf.where(col("DAMAGED_PROPERTY").isNull)

  def checkF(str: String) = {
    val field = str.split(" ")

    if (field(0).startsWith("DAMAGED") && field(1).toInt >= 4) true else false

  }
  val checkUDF = udf(checkF(_: String): Boolean)
  
  val damagedDF1 = noDamageDF.withColumn("Check", checkUDF(col("VEH_DMAG_SCL_2_ID")))
  
  val damagedDF2 = noDamageDF.withColumn("Check", checkUDF(col("VEH_DMAG_SCL_1_ID"))).union(damagedDF1).filter(col("Check") === true)

  val insuranceDF = damagedDF2.filter(col("FIN_RESP_TYPE_ID").isin("PROOF OF LIABILITY INSURANCE","LIABILITY INSURANCE POLICY","INSURANCE BINDER"))
                   .drop("Check")

  val finalDf = insuranceDF.select("CRASH_ID").distinct().count()

  print(finalDf)

  spark.stop()
}
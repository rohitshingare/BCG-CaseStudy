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
// Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
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

  val noDamageDF = damagesDf.where(col("DAMAGED_PROPERTY").like("%NO DAMAGE%"))

  val joinCondition = unitDf.col("CRASH_ID") === noDamageDF.col("CRASH_ID")

  val joinType = "inner"

  val JoinedDf = unitDf.join(broadcast(noDamageDF), joinCondition, joinType).drop(unitDf.col("CRASH_ID"))

  def checkF(str: String) = {
    val field = str.split(" ")

    if (field(0).startsWith("DAMAGED") && field(1).toInt > 4) true else false

  }
  val checkUDF = udf(checkF(_: String): Boolean)

  val sevDF1 = JoinedDf.withColumn("check", checkUDF(col("VEH_DMAG_SCL_1_ID")))

  val sevDF2 = sevDF1.withColumn("check", checkUDF(col("VEH_DMAG_SCL_2_ID")))
    .union(sevDF1)
    .filter(col("check") === true)

  val insuranceDF = sevDF2.filter(col("FIN_RESP_TYPE_ID").isin("PROOF OF LIABILITY INSURANCE", "LIABILITY INSURANCE POLICY", "INSURANCE BINDER"))
    .drop("check")

  val finalDf = insuranceDF.select("CRASH_ID").distinct().count()

  print(finalDf)

  spark.stop()
}

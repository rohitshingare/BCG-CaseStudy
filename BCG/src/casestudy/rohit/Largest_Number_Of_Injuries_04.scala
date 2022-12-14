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

  def addM(total_injury: Int, total_death: Int) = {
    total_injury + total_death
  }
  val totalUDF = udf(addM(_: Int, _: Int): Int)

  val df1 = readerDf.withColumn("total_count", totalUDF(col("TOT_INJRY_CNT"), col("DEATH_CNT")))

  val df2 = df1.groupBy("VEH_MAKE_ID")
    .agg(sum("total_count").as("total_sum"))
    .sort(col("total_sum").desc)

  val df3 = df2.withColumn("row_number", row_number().over(Window.orderBy(col("total_sum").desc)))

  val df4 = df3.filter(col("row_number") >= 5 && col("row_number") <= 15)
    .drop(col("row_number"))
    .drop(col("total_sum"))

  df4.show()

  spark.close()

}
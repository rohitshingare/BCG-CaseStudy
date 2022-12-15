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

object Mens_Died_In_Crashes extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "mens died in crashes")
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

  readerDf.printSchema()

  print("data has :" + readerDf.rdd.getNumPartitions);
  val readerReparDf = readerDf.repartition(8)
  print("data has :" + readerReparDf.rdd.getNumPartitions);

  val diedMaleDf = readerReparDf.where(col("DEATH_CNT") > 0 && col("PRSN_GNDR_ID") === "MALE")

  diedMaleDf.select(count("*").as("total_number_crashes")).show()

  //Another way

  //val anotherDiedMaleDf = readerDf.where(col("PRSN_D EATH_TIME").isNotNull && col("PRSN_GNDR_ID") === "MALE")
  //anotherDiedMaleDf.select(count("*").as("total_number_crashes")).show()

  spark.close()

}
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

object Ethnic_User_Group extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", " top ethnic user group of each unique body style ")
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

  val df1 = readerDf.where(!col("VEH_BODY_STYL_ID").isin("NA", "UNKNOWN"))
    .groupBy(col("VEH_BODY_STYL_ID"), col("VEH_MAKE_ID"))
    .agg(count("VEH_BODY_STYL_ID").as("total"))
    .sort(col("total").desc)
  df1.show()

  val df2 = df1.withColumn("top", first("total").over(Window.partitionBy(col("VEH_BODY_STYL_ID")).orderBy(col("total").desc)))
  df2.show(1000)

  val df3 = df2.select(col("VEH_MAKE_ID")).filter(col("total") === col("top")).drop("total").drop("top")
  df3.show()

  val df4 = df3.orderBy(col("VEH_MAKE_ID")).distinct()
  
  df4.show()

  spark.close()

}
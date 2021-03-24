package Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Distinct
import org.apache.spark.sql.functions._

object HealthCampDataAnalysis extends App{
  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkSession using every core of the local machine, named RatingsCounter
  val spark = SparkSession
    .builder
    .appName("Test1")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  val First_camp = "data/First_Health_Camp_Attended.csv"
  val Second_camp = "data/Second_Health_Camp_Attended.csv"
  val Third_camp = "data/Third_Health_Camp_Attended.csv"
  val Camp_detail = "data/Health_Camp_Detail.csv"
  val patient_profile = "data/Patient_Profile.csv"

  val camp_detail_df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(Camp_detail)

  camp_detail_df.printSchema()
  camp_detail_df.show(2)

  val camp_detail_df1 = camp_detail_df
    .withColumn("Start_Date_Camp",to_date(unix_timestamp(col("Camp_Start_Date"), "dd-MMM-yy").cast("timestamp")))
    .withColumn("End_Date_Camp",to_date(unix_timestamp(col("Camp_End_Date"), "dd-MMM-yy").cast("timestamp")))
    .withColumn("No_of_Days",datediff(col("End_Date_Camp"), col("Start_Date_Camp")))
    .withColumn("Start_Month",date_format(to_date($"Start_Date_Camp", "yyyy-mm-dd"),"MM"))
    .withColumn("End_Month",date_format(to_date($"End_Date_Camp", "yyyy-mm-dd"),"MM"))
    .withColumn("Start_Year",date_format(to_date($"Start_Date_Camp", "yyyy-mm-dd"),"yyyy"))
    .withColumn("End_Year",date_format(to_date($"End_Date_Camp", "yyyy-mm-dd"),"yyyy"))
    .drop("Camp_Start_Date","Camp_End_Date", "Category1","Category2","Category3")

  camp_detail_df1.show(2)

  println("Unique Health Camp Id's")
  camp_detail_df1.agg(countDistinct("Health_Camp_ID")as("Unique_Health_Camp_Id")).show()


  println("Which health camp stayed Long, printing top 5")
  camp_detail_df1.groupBy("Health_Camp_ID").agg(sum("No_of_Days")as("Days")).orderBy($"Days".desc).show(5)

  println("Which month is most started, printing top 5")

  camp_detail_df1.groupBy("Start_Month").agg(count("Health_Camp_ID")as("No_of_camps")).orderBy($"No_of_camps".desc).show(5)

  println("Which month is most End, printing top 5")

  camp_detail_df1.groupBy("End_Month").agg(count("Health_Camp_ID")as("No_of_camps")).orderBy($"No_of_camps".desc).show(5)

  println("Which year is most started, printing top 5")

  camp_detail_df1.groupBy("Start_Year").agg(count("Health_Camp_ID")as("No_of_camps")).orderBy($"No_of_camps".desc).show(5)

  println("Which year is most End, printing top 5")

  camp_detail_df1.groupBy("End_Year").agg(count("Health_Camp_ID")as("No_of_camps")).orderBy($"No_of_camps".desc).show(5)
}

package Test
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.Distinct
import org.apache.spark.sql.functions._
import shapeless.syntax.std.tuple.unitTupleOps


object Third_Camp_Data_Analysis extends App{
  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  //Create a SparkSession using every core of the local machine, named RatingsCounter
  val spark = SparkSession
    .builder
    .appName("Test1")
    .master("local[*]")
    .getOrCreate()


  import spark.implicits._

  val Third_camp = "data/Third_Health_Camp_Attended.csv"

  val Third_df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(Third_camp)
    .drop("_c4")

  Third_df.printSchema()
  Third_df.show(2)

  println("Unique Patient Id's In Third Camp")
  Third_df.agg(countDistinct("Patient_ID")as("Unique_Patient_Id")).show()



  println(" Which is most last visited stall in 3rd Camp? ")
  Third_df.groupBy("Last_Stall_Visited_Number").agg(count("Last_Stall_Visited_Number")as("Count_Of_Last_Visited_Stall")).orderBy($"Count_Of_Last_Visited_Stall".desc).show()



}

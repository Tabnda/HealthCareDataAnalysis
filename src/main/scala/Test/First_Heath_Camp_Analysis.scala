package Test
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.Distinct
import org.apache.spark.sql.functions._
import shapeless.syntax.std.tuple.unitTupleOps


object First_Heath_Camp_Analysis extends App{

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  //Create a SparkSession using every core of the local machine, named RatingsCounter
  val spark = SparkSession
    .builder
    .appName("Test1")
    .master("local[*]")
    .getOrCreate()


  import spark.implicits._

  val First_camp = "data/First_Health_Camp_Attended.csv"

  val First_df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(First_camp)
    .drop("_c4")

  First_df.printSchema()
  First_df.show(2)

  println("Unique Patient Id's In First Camp")
  First_df.agg(countDistinct("Patient_ID")as("Unique_Patient_Id")).show()


  println("Which camp / Patient got more donation?")
  First_df.groupBy("Patient_ID").agg(sum("Donation")as("Donation")).orderBy($"Donation".desc).show(5)



}

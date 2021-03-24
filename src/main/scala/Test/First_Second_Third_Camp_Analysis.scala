package Test
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.Distinct
import org.apache.spark.sql.functions._
import shapeless.syntax.std.tuple.unitTupleOps


object First_Second_Third_Camp_Analysis extends App{
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
  val Second_camp = "data/Second_Health_Camp_Attended.csv"
  val Third_camp = "data/Third_Health_Camp_Attended.csv"
  val Camp_detail = "data/Health_Camp_Detail.csv"
  val patient_profile = "data/Patient_Profile.csv"

  val First_df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(First_camp)
    .drop("_c4","Donation")


  First_df.printSchema()
  First_df.show(2)


  val Second_df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(Second_camp)

  Second_df.printSchema()
  Second_df.show(2)


  val Third_df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(Third_camp)
    .drop("Number_of_stall_visited","Last_Stall_Visited_Number")
    .withColumn("HealthScore",lit(0))

   Third_df.printSchema()
   Third_df.show(2)

   val union = First_df.union(Second_df).union(Third_df).distinct()
   union.show(7)

  println("Which Camp has been attended most?")
  //union.groupBy("Health_Camp_ID","Patient_ID").count().orderBy("Health_Camp_ID").show(10)
  union.groupBy("Health_Camp_ID")
    .agg(count("Patient_ID")as("Count_Of_Camp_Attended_Most"))
    .orderBy($"Count_Of_Camp_Attended_Most".desc).show()





  //println(" People attending all the three camps ")
  //println(union.select("Patient_ID").count())






}

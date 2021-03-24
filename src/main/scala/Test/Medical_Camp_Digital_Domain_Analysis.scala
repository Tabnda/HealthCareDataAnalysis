package Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import shapeless.syntax.std.tuple.unitTupleOps
import org.apache.spark.ml.feature.Imputer


object Medical_Camp_Digital_Domain_Analysis extends App{
  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkSession using every core of the local machine, named RatingsCounter
  val spark = SparkSession
    .builder
    .appName("Test1")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._
  val patient_profile = "data/Patient_Profile.csv"
  val First_camp = "data/First_Health_Camp_Attended.csv"
  val Second_camp = "data/Second_Health_Camp_Attended.csv"
  val Third_camp = "data/Third_Health_Camp_Attended.csv"

  val Patient_df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(patient_profile)
    .drop("Income","Education_Score","Age","First_Interaction","City_Type","Employer_Category")
    .withColumnRenamed("Patient_ID","Patient_ID1")

  Patient_df.printSchema()
  Patient_df.show(2)


  val First_camp_df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(First_camp)
    .drop("Donation","Health_Score","_c4")

  First_camp_df.printSchema()
  First_camp_df.show(2)


  val Second_camp_df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(Second_camp)
    .drop("Health Score")

  Second_camp_df.printSchema()
  Second_camp_df.show(2)


  val Third_camp_df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(Third_camp)
    .drop("Number_of_stall_visited" , "Last_Stall_Visited_Number")

  Third_camp_df.printSchema()
  Third_camp_df.show(2)


   val union = First_camp_df.union(Second_camp_df).union(Third_camp_df).distinct()
   union.show(7)

  val joiningtwotable = union.join(Patient_df, Patient_df.col("Patient_ID1") === union.col("Patient_ID"),"inner")
  val FinalJoined = joiningtwotable.drop("Patient_ID1","Patient_ID")
  FinalJoined.show(10)

  //FinalJoined.withColumn("Sum_Of_Digitally_Shared_domain",FinalJoined.groupBy("Health_Camp_ID").agg(sum("Online_Follower"),sum("LinkedIn_Shared"),sum("Twitter_Shared"),sum("Facebook_Shared")))


  //val newDf = FinalJoined.select(colsToSum.map(col).reduce((c1, c2) => c1 + c2) as "sum")
  //val sum1 =  FinalJoined.withColumn("Sum_Of_Shared_Posts", expr("Online_Follower+LinkedIn_Shared+Twitter_Shared+Facebook_Shared"))
  //sum1.show(60)
  //val Maximum = sum1.groupBy("Health_Camp_ID").agg(sum("Sum_Of_Shared_Posts")as("Sum_Of_Shared_Posts"))
  //println("Medical Camp that has Shared Most posts...")
  //Maximum.orderBy($"Sum_Of_Shared_Posts".desc).show()

}

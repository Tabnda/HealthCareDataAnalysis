package Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import shapeless.syntax.std.tuple.unitTupleOps
import org.apache.spark.ml.feature.Imputer



object Patient_Profile_Analysis extends App{
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

  val patient_profile_df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(patient_profile)
    .drop("Online_Follower","LinkedIn_Shared","Twitter_Shared","Facebook_Shared","First_Interaction","City_Type","Employer_Category")
    .withColumnRenamed("Patient_ID","Patient_ID1")

  val First_camp_df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(First_camp)
    .drop("Donation","_c4")

  val Second_camp_df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(Second_camp)

  println("Printing the union of First and Second Table")
  val union = First_camp_df.union(Second_camp_df).distinct()
  union.show(7)



  println("Printing Schema")
  patient_profile_df.printSchema()

  println("Printing Patient Profile which has null values")
  patient_profile_df.show(10)


  println("Converting the type of the Columns and printing the Schema")
  val patient_profile_df1 = patient_profile_df.withColumn("Age",col("Age").cast(IntegerType))
    .withColumn("Income",col("Income").cast(IntegerType))
    .withColumn("Education_Score",col("Education_Score").cast(IntegerType))
  patient_profile_df1.printSchema()

  println("Replacing all None Values in Age , Income , Education Score with mean values of the column")
  val imputer = new Imputer()
    .setStrategy("mean")
    .setMissingValue(0)
    .setInputCols(Array("Education_Score","Age","Income"))
    .setOutputCols(Array("Education_Score","Age","Income"))

  val model = imputer.fit(patient_profile_df1)
  val model1  = model.transform(patient_profile_df1)


  val joiningtwotable = model1.join(union, model1.col("Patient_ID1") === union.col("Patient_ID"),"inner")
  val FinalJoined = joiningtwotable.drop("Patient_ID1")
  FinalJoined.show(10)


 // union.join(union,model("Patient_ID") ===  union("Patient_ID"),"inner")
   // .show(false)



  //import spark.implicits._
  println("Which age group people having the better / worse health score")
  val interval = 10
  val Data1 = FinalJoined.withColumn("Age-range", $"Age" - ($"Age" % interval))
    .withColumn("Age-range", concat($"Age-range", lit(" - "), $"Age-range" + interval)) //optional one
  //Data1.select("Age-range","Age").orderBy("Age").show
  val test = Data1.groupBy("Age-range","Health_Score").count().orderBy($"Age-range".asc,$"count".desc)
  test.select("Age-range","Health_Score").show
  //val w = Window.partitionBy("Age-range").orderBy($"Age-range".asc,$"count".desc)
  //val result = test.withColumn("index", row_number().over(w))
  //result.orderBy($"Age-range".asc,$"count".desc).filter($"index"<=3 ).show(false)

  println("Which Income Range people having the better / worse education score")
  val interval1 = 10
  val Data2 = FinalJoined.withColumn("Income-range", $"Income" - ($"Income" % interval1))
    .withColumn("Income-range", concat($"Income-range", lit(" - "), $"Income-range" + interval1)) //optional one
  //Data1.select("Income-range","Income").orderBy("Income").show
  val test1 = Data2.groupBy("Income-range","Education_Score").count().orderBy($"Income-range".asc,$"count".desc)
  test1.select("Income-range","Education_Score").show
  //val w = Window.partitionBy("Income-range").orderBy($"Income-range".asc,$"count".desc)
  //val result = test.withColumn("index", row_number().over(w))
  //result.orderBy($"Income-range".asc,$"count".desc).filter($"index"<=3 ).show(false)





}

package part2dataframes

import org.apache.spark.sql.SparkSession

object DataFramesBasicsExercises extends App {

  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val phones = Seq(
    ("Samsung", "S21", 6.2, 12),
    ("Apple", "Iphone 11", 6.1, 12)
  )
  val phonesDF = phones.toDF("Make", "Model", "ScreenSize", "CameraMP")
  phonesDF.printSchema()

}

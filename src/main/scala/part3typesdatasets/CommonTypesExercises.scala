package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import part3typesdatasets.CommonTypes.spark

object CommonTypesExercises extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  def getCarNames: List[String] = List("volkswagen", "wv")

  val carNamesFilter = getCarNames
    .map(carName => col("Name").contains(carName))
    .reduce((f1, f2) => f1 or f2)

  carsDF
    .select("Name")
    .where(carNamesFilter)
    .show()

}

package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

object DatasetsExercises extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/cars.json")

  import spark.implicits._
  val carsDS = carsDF.as[Car]

  // count how many cars we have
  val numberOfCars = carsDS.count()
  println(numberOfCars)

  // count how many powerful cars we have (HP > 140)
  println(
    carsDS.filter(_.Horsepower.exists(v => v > 140))
      .count()
  )

  // average HP for the entire dataset
  println(
    carsDS.map(_.Horsepower.getOrElse(0L))
      .reduce(_ + _)
  )
}

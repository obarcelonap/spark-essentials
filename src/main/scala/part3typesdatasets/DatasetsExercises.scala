package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.array_contains

object DatasetsExercises extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

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

  val carsDF = readDF("cars.json")

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

  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]

  guitarPlayersDS.joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
    .show()

}

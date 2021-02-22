package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnsAndExpressionsExercises extends App {

  val spark = SparkSession.builder()
    .appName("Columns and expressions exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .format("json")
    .load("src/main/resources/data/movies.json")

  moviesDF
    .select(col("Title"), expr("Major_Genre"))
    .show()

  moviesDF
    .selectExpr("Title", "US_Gross + Worldwide_Gross")
    .show()

  moviesDF
    .selectExpr("Title", "Major_Genre", "IMDB_Rating")
    .filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
    .show()
}

package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationsExercises extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")

  moviesDF
    .selectExpr("sum(US_Gross + Worldwide_Gross)")
    .show()

  moviesDF
    .select(countDistinct(col("Director")))
    .show()

  moviesDF
    .select(
      mean("US_Gross"),
      stddev("US_Gross"),
    )
    .show()

  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("avg_IMDB_Rating"),
      sum("US_Gross").as("sum_US_Gross"),
    )
    .sort(col("avg_IMDB_Rating").desc_nulls_last)
    .show()
}

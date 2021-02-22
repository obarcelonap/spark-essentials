package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import part2dataframes.DataSources.{carsDF, spark}

object DataSourcesExercises extends App {

  // creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesSchema = StructType(Array(
    StructField("Title", StringType),
    StructField("US_Gross", LongType),
    StructField("Worldwide_Gross", LongType),
    StructField("US_DVD_Sales", StringType),
    StructField("Production_Budget", LongType),
    StructField("Release_Date", DateType),
    StructField("MPAA_Rating", StringType),
    StructField("Running_Time_min", StringType),
    StructField("Distributor", StringType),
    StructField("Source", StringType),
    StructField("Major_Genre", StringType),
    StructField("Creative_Type", StringType),
    StructField("Director", StringType),
    StructField("Rotten_Tomatoes_Rating", StringType),
    StructField("IMDB_Rating", DoubleType),
    StructField("IMDB_Votes", LongType),
  ))

  // reading a DF
  val moviesDF = spark.read
    .format("json")
    .schema(moviesSchema)
    .load("src/main/resources/data/movies.json")

  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies.csv")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet")

  moviesDF.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()
}

package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max

object JoinsExercises extends App {

  val spark = SparkSession.builder()
    .appName("Joins exercises")
    .config("spark.master", "local")
    .getOrCreate()

  def loadTableDF(table: String) = spark.read
    .format("jdbc")
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "user" -> "docker",
      "password" -> "docker",
      "dbtable" -> s"public.$table"
    ))
    .load()

  val employeesDF = loadTableDF("employees")
  val salariesDF = loadTableDF("salaries")

  // show all employees and their max salary
  val maxEmployeeSalaryDF = salariesDF
    .groupBy("emp_no")
    .agg(max("salary").as("max_salary"))


  employeesDF
    .join(maxEmployeeSalaryDF, "emp_no")
    .show()

  val deptManagerDF = loadTableDF("dept_manager")

  // show all employees who where never managers
  employeesDF
    .join(deptManagerDF, employeesDF.col("emp_no") === deptManagerDF.col("emp_no"), "anti")
    .show()

  val top10SalariesDF = maxEmployeeSalaryDF
    .sort(maxEmployeeSalaryDF.col("max_salary").desc_nulls_last)
    .limit(10)

  val top10BestPaidEmployees = employeesDF
    .join(top10SalariesDF, employeesDF.col("emp_no") === top10SalariesDF.col("emp_no"), "semi")

  val titlesDF = loadTableDF("titles")

  // find the job titles of the best paid 10 employees in the company
  titlesDF
    .join(top10BestPaidEmployees, titlesDF.col("emp_no") === top10BestPaidEmployees.col("emp_no"), "semi")
    .select("title")
    .distinct()
    .show()

}

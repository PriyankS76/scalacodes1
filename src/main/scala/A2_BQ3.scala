import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object A2_BQ2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "Priyank")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")
    //
    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val work_hours = List(
      (1, "2024-01-10",9, "Sales"),
      (2, "2024-01-11",7, "Support"),
      (3, "2024-01-12",8, "Sales"),
      (4, "2024-01-13 ",10, "Marketing"),
      (5, "2024-01-14",5, "Sales"),
      (6, "2024-01-15",6, "Support"))
      .toDF("employee_id", "work_date", "hours_worked", "department")

    val df1 = work_hours.select(col("employee_id"), col("work_date"), col("hours_worked"), col("department"),
      when(col("hours_worked") > 8, ("Overtime"))
        .when(col("hours_worked") <= 8, ("regular")).alias("Hours_Category"))
    df1.show()

    val df2 = df1.filter(col("department").startsWith("S"))
    df2.show()

    df2.groupBy("department").agg(sum(col("hours_worked")),avg(col("hours_worked")),max(col("hours_worked")),min(col("hours_worked"))).show()

    scala.io.StdIn.readLine()
  }
}

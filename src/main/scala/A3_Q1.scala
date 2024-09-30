import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, lag, month, when}

object A3_Q1 {
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
//
//    Create a new column review_month using month extracted from review_date.
//     Filter records where the position ends with 'Manager' and the performance_score is greater
//    than 80.
//

    val emp_per = List(
      ("E001","Sales",85,"2024-02-10","Sales Manager"),
      ("E001","Sales",87,"2024-03-10","Sales Manager"),
      ("E002","HR", 78,"2024-03-15","HR Assistant"),
      ("E003","IT",92,"2024-01-22","IT Manager"),
      ("E003","IT",92,"2024-02-22","IT Manager"),
      ("E004","Sales",88,"2024-02-18","Sales Rep"),
      ("E005","HR",95,"2024-03-20","HR Manager")
      ).toDF("employee_id", "department", "performance_score", "review_date", "position")

    val df1 = emp_per.filter(col("position").endsWith("Manager") && col("performance_score")>80).show()
    val extmonth = emp_per.withColumn("review_month", month($"review_date"))
      extmonth.show()

    val df2 = extmonth.groupBy("department","review_month").agg(avg(col("performance_score")).alias("avg"))
      df2.show()



    val count_emp = extmonth.groupBy("department","review_month").agg(count(when(col("performance_score") > 90, 1)).alias("high_performers_count"))
    .show()

    val emp_window = Window.partitionBy("department").orderBy("review_date")
    val emp_lag = emp_per.withColumn("previous_performance", lag("performance_score", 1)
      .over(emp_window))

    val emp_improvement = emp_lag.withColumn("performance_improvement", col("performance_score") - col("previous_performance"))

    emp_improvement.show()

//     Group by department and review_month, and calculate:
    //      o The average performance_score per department per month.
    //    o The count of employees who received a performance_score above 90.
    //     Use the lag function to calculate the performance improvement or decline for each
    //    employee compared to their previous review

  }
}

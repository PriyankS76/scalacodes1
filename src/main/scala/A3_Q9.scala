import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object A3_Q9 {
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

    val employee_attendance = List(
      ("A001", "E001", "2023-11-01", "Present", "Sales", "Day"),
      ("A002", "E002", "2023-11-02", "Absent", "HR", "Night"),
      ("A003", "E003", "2023-11-03", "Present", "IT", "Night"),
      ("A004", "E001", "2023-11-04", "Absent", "Sales", "Night"),
      ("A005", "E002", "2023-11-05", "Present", "HR", "Day"),
      ("A006", "E003", "2023-11-06", "Absent", "IT", "Night"),
      ("A007", "E001", "2023-11-07", "Present", "Sales", "Day"),
      ("A008", "E002", "2023-11-08", "Absent", "HR", "Night"),
      ("A009", "E003", "2023-11-09", "Present", "IT", "Night"),
      ("A010", "E001", "2023-11-10", "Absent", "Sales", "Night")
    ).toDF("attendance_id", "employee_id", "attendance_date", "status", "department", "shift")

//     Create a new column attendance_month using month extracted from attendance_date.
//    Filter records where status is 'Absent' and the shift is 'Night'.
//     Group by department and attendance_month, and calculate:
//      o The total count of 'Absent' days.
//      o The average number of 'Absent' days per employee.
//      o The maximum continuous absence streak for any employee.
//     Use the lead function to predict the next attendance status for each employee

    val extmonth = employee_attendance.withColumn("attendance_month", year($"attendance_date"))
    extmonth.show()
    val df1 = extmonth.filter(col("status") === "Absent" && col("shift")=== "Night")
    df1.show()

    val df2 = extmonth.filter(col("status") === "Absent")
    df2.show()

//    val df3 = df2.groupBy("department","attendance_month").agg(sum(when(col("status") === "Absent",1)).as("count"),
//      avg(when(col("status") === "Absent",1)).as("avg"))
//     df3.show()

    val df3 = extmonth.groupBy("department", "attendance_month")
      .agg(
        sum(when(col("status") === "Absent", 1).otherwise(0)).as("total_absent_days"),
        avg(when(col("status") === "Absent", 1).otherwise(0)).as("avg_absent_days_per_employee"))
       df3.show()

    val df4 = Window.partitionBy("employee_id").orderBy("attendance_date")
    val df5 = extmonth.withColumn("Next_status", lead("status",1).over(df4))


//      val df4 = df3.functions.max(sum(col("status"))).over(Window.partitionBy("employee_id").orderBy("attendance_date")))
//      df4.show()


  }
}

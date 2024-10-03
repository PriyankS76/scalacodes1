import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}

object A3_Q7 {
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

    val patient_visits = List(
      ("V001","P001", "2024-03-01", "John Doe", 700, "Cardiology"),
      ("V002","P002", "2024-03-02", "Jane Smith", 400, "Dentistry"),
      ("V003","P003", "2024-03-01", "Mike Johnson", 600, "Dermatology"),
      ("V004","P004", "2024-03-03", "Emily Davis", 900, "General Medicine"),
      ("V005","P005", "2024-03-02", "Chris Brown", 550, "Ophthalmology"),
      ("V006","P006", "2024-03-01", "Anna Wilson", 800, "Laboratory"),
      ("V007","P007", "2024-03-04", "David Lee", 850, "Allergy"),
      ("V008","P008", "2024-03-05", "Sarah Miller", 670, "Cardiology"),
      ("V009","P009", "2024-03-02", "Jason Taylor", 650, "General Medicine"),
      ("V010","P010", "2024-03-03", "Olivia Moore", 450, "Dermatology")
    ).toDF("visit_id", "patient_id", "visit_date", "doctor_name", "treatment_cost", "department")

//    You have a DataFrame patient_visits with columns: visit_id, patient_id, visit_date, doctor_name,
//    treatment_cost, and department.
//     Create a new column visit_year using year extracted from visit_date.
//     Filter records where department starts with 'Cardio' and the treatment_cost is greater than
//      500.
//     Group by doctor_name and visit_year, and calculate:
//      o The total treatment_cost per doctor per year.
//    o The maximum treatment_cost for any visit.
//    o The count of visits per doctor.
//     Use the lead function to predict the next visit's treatment cost for each patient

    val extyear = patient_visits.withColumn("visit_year", year($"visit_date"))
    extyear.show()

    val df1 = extyear.filter(col("department").startsWith("Cardio") && col("treatment_cost")>=500)
    df1.show()
    //
    val df2 = extyear.groupBy("doctor_name","visit_year").agg(sum("treatment_cost"),functions.max("treatment_cost"),count("visit_id"))
    df2.show()

    val df3 = Window.partitionBy("patient_id").orderBy("treatment_cost")
    val change_analysis = extyear.withColumn("Next_visit cost", lead("treatment_cost", 1).over(df3))
    change_analysis.show()


  }
}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}

object A1_SQ5 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .appName("Priyank")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function

    val data = List((1, "2024-07-27"), (2, "2024-12-25"), (3, "2025-01-01")).toDF("event_id", "date") //toDF provides column name


//        ,
    val df1 = data.select(col("event_id"), col("date"), when(col("date").isin("2024-12-25", "2025-01-01"), lit("True")).otherwise("False").as("Is_holiday")).show()

  }
}

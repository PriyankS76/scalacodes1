import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, when}

object A1_MQ3 {
  def main(args: Array[String]): Unit = {
  Logger.getLogger(("org")).setLevel(Level.OFF)
  Logger.getLogger(("akka")).setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .appName("Priyank")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._ // for toDF function

//  Question: How would you add a new column season with values "Summer" if order_date is in June,
//  July, or August, "Winter" if in December, January, or February, and "Other" otherwise?

  val order1 = List((1, "2024-07-01"),
    (2, "2024-12-01"),
    (3, "2024-05-01")).toDF("order_id", "order_date") //toDF provides column name

  val extracteddate = order1.withColumn("month", month($"order_date"))

    val df = extracteddate.select(col("order_id"), col("order_date"), when(col("month") === 6 || col("month") === 7 || col("month") === 8 , ("Summer"))
      .when(col("month") === 12 || col("month") === 1 || col("month") === 2 , ("Winter")).otherwise("Other").alias("Season")).show()

  scala.io.StdIn.readLine()
}
}
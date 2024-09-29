import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, to_timestamp, when}

object A1_MQ5 {
  def main(args: Array[String]): Unit = {


    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .appName("Priyank")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function

    val logins = List(
      (1, "09:00"),
      (2, "18:30"),
      (3, "14:00")
    ).toDF("login_id", "login_time") //toDF provides column name

    val extractedtime = logins.withColumn("time", to_timestamp($"login_time"))

    logins.printSchema()
    extractedtime.printSchema()

    val df = extractedtime.select(col("login_id"), col("login_time"), when(col("login_time").lt("12:00:00") ,lit("true") )
      .otherwise(lit("false")).alias("Is_morning")).show()

    scala.io.StdIn.readLine()
  }
  }

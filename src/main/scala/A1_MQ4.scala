import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object A1_MQ4 {
  def main(args: Array[String]): Unit = {


    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .appName("Priyank")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function

    val inventory = List((1,100), (2,1500), (3,300)).toDF("sale_id", "amount") //toDF provides column name

    inventory.select(col("sale_id"), col("amount"),
      when(col("amount") < 200, ("0"))
        .when(col("amount") >= 200 && col("amount") < 1000, ("10"))
        .otherwise("20").alias("Discount value")).show()

    scala.io.StdIn.readLine()
  }
}

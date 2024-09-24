import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object A_MQ1 {
  def main(args: Array[String]): Unit = {


    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .appName("Priyank")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function

    val inventory = List((1, 5), (2, 15), (3, 25)).toDF("item_id", "quantity") //toDF provides column name

    inventory.select(col("item_id"), col("quantity"), when(col("quantity") < 10, ("low")).when(col("quantity") >= 10 && col("quantity") < 20, ("Medium")).otherwise("High").alias("Price_Range")).show()

    scala.io.StdIn.readLine()
  }
}

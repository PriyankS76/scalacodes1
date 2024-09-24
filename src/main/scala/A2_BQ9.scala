import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object A2_BQ9 {
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

     val purchase_history = List(
      (1,1,2500, "2024-01-05"),
      (2,2,1500, "2024-01-15"),
      (3,3,500, "2024-02-20"),
      (4,4,2200, "2024-03-01"),
      (5,5,900, "2024-01-25"),
      (6,6,3000, "2024-03-12"))
      .toDF("pur_id", "cus_id", "amt", "pr_date")

    val df1 = purchase_history.select(col("pur_id"), col("cus_id"), col("amt"), col("pr_date"),
      when(col("amt") > 2000, ("Large"))
        .when(col("amt") > 1000 && col("amt") < 2000, ("Medium"))
        .otherwise("Small").alias("Pur_Category"))
    df1.show()


    df1.groupBy("Pur_category").agg(sum(col("amt")),avg(col("amt")),max(col("amt")),min(col("amt"))).show()
    scala.io.StdIn.readLine()
  }
}

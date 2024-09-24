import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object A2_BQ5 {
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

     val trData = List(
      (1,1,1200, "2024-01-15"),
      (2,2,600, "2024-01-20"),
      (3,3,300, "2024-02-15"),
      (4,4,1500, "2024-02-20"),
      (5,5,200, "2024-04-05"),
      (6,6,900, "2024-03-12"))
      .toDF("transaction_id", "cus_id", "amt", "trans_date")

    val df1 = trData.select(col("transaction_id"), col("cus_id"), col("amt"), col("trans_date"),
      when(col("amt") > 1000, ("High"))
        .when(col("amt") > 500 && col("amt") < 1000, ("Medium"))
        .otherwise("Low").alias("Amt_Category"))
    df1.show()

    // Create a new column `transaction_month` from `transaction_date`
    val df2 = df1.withColumn("transaction_year", year(to_date(col("trans_date"), "yyyy-MM-dd")))
    df2.show()


    val df3 = df2.filter(col("transaction_year") === 2024)
    df3.show()

    df1.groupBy("Amt_category").agg(sum(col("amt")),avg(col("amt")),max(col("amt")),min(col("amt"))).show()
    scala.io.StdIn.readLine()
  }
}

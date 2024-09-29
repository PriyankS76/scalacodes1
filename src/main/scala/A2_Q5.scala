import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object A2_Q5 {
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
      (1, "2023-12-01",1200, "Credit"),
      (2, "2023-11-15",600, "Dedit"),
      (3, "2023-12-20",300, "Credit"),
      (4, "2023-10-10",1500, "Dedit"),
      (5, "2023-12-30",250, "Credit"),
      (6, "2023-09-25",700, "Dedit"))
      .toDF("transaction_id", "trans date", "amt", "trans_type")

    val df1 = trData.select(col("transaction_id"), col("trans date"), col("amt"), col("trans_type"),
      when(col("amt") > 1000, ("High"))
        .when(col("amt") > 500 && col("amt") < 1000, ("Medium"))
        .otherwise("Low").alias("Amt_Category"))
    df1.show()

    // Create a new column `transaction_month` from `transaction_date`
    val df2 = df1.withColumn("transaction_month", month(to_date(col("trans date"), "yyyy-MM-dd")))
    df2.show()


   val df3 = df2.filter(col("transaction_month") === 12)
    df3.show()

    df1.groupBy("trans_type").agg(sum(col("amt")),avg(col("amt")),max(col("amt")),min(col("amt"))).show()
    scala.io.StdIn.readLine()
  }
}

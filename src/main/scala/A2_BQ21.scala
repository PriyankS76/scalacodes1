import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max, min, sum, when}

object A2_BQ21 {
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

    val utility_bills = List(
      (1,1,250, "2024-02-05"),
      (2,2,80, "2024-02-10"),
      (3,3,150, "2024-02-15"),
      (4,4,220, "2024-02-20"),
      (5,5,90, "2024-02-25"),
      (6,6,300, "2024-02-28"))
      .toDF("bill_id", "cus_id", "amt", "billing_date")


    val df1 = utility_bills.select(col("bill_id"), col("cus_id"), col("amt"), col("billing_date"),
      when(col("amt") > 200, ("High"))
        .when(col("amt") >= 100 && col("amt") <= 200, ("Medium"))
        .otherwise("Low").alias("bill_status"))
    df1.show()


    df1.groupBy("bill_status").agg(sum(col("amt")),avg(col("amt")),max(col("amt")),min(col("amt"))).show()
    scala.io.StdIn.readLine()
  }
}

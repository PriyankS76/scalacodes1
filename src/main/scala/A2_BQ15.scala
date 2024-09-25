import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max, min, sum, when}

object A2_BQ15 {
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

    val purchase = List(
      (1,1,700, "2024-02-05"),
      (2,2,150, "2024-02-10"),
      (3,3,400, "2024-02-15"),
      (4,4,600, "2024-02-20"),
      (5,5,250, "2024-02-25"),
      (6,6,1000, "2024-02-28"))
      .toDF("pur_id", "cus_id", "amt", "pr_date")


    val df1 = purchase.select(col("pur_id"), col("cus_id"), col("amt"), col("pr_date"),
      when(col("amt") > 500, ("Large"))
        .when(col("amt") > 200 && col("amt") < 500, ("Medium"))
        .otherwise("Small").alias("Pur_status"))
    df1.show()


    df1.groupBy("Pur_status").agg(sum(col("amt")),avg(col("amt")),max(col("amt")),min(col("amt"))).show()
    scala.io.StdIn.readLine()
  }
}

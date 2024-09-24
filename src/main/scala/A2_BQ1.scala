import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object A2_BQ1 {
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


    val cusData = List(
      (1, "2024-01-10",4, "Great service!"),
      (2, "2024-01-15",5, "Excellent!"),
      (3, "2024-02-20",2, "Poor experience"),
      (4, "2024-02-25 ",3, "Good value"),
      (5, "2024-03-05",4, "Great quality"),
      (6, "2024-03-12",1, "Bad service"))
      .toDF("customer_id", "feedback_date", "rating", "feedback_text")


    val df1 = cusData.select(col("customer_id"), col("feedback_date"), col("rating"), col("feedback_text"),
      when(col("rating") > 5, ("Excellent"))
        .when(col("rating") > 3 && col("rating") < 5, ("Good"))
        .otherwise("Poor").alias("Rating_Category"))
    df1.show()

    val df2 = df1.filter(col("feedback_text").startsWith("Great")).show

    val df3 = df1.withColumn("feedback_month", month(to_date(col("feedback_date"), "yyyy-MM-dd")))
    df3.show()


    df3.groupBy("feedback_month").agg(avg(col("rating")),max(col("rating")),min(col("rating"))).show()

    scala.io.StdIn.readLine()
  }
}

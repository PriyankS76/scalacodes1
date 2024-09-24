import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, when}

object A2_SQ7 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "Priyank")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

     val product_review = Seq(
      (1,"Smartphone",4, "2024-01-15"),
      (2,"Speaker",3, "2024-01-20"),
      (3,"Smartwatch",5,"2024-01-20"),
      (4,"Screen",2,"2024-02-20"),
      (5,"Speakers",4,"2024-03-05"),
      (6,"Soundbar",3,"2024-03-12"))
      .toDF("review_id","product_name", "rating" ,"review_date")

    val df1= product_review.select(col("review_id"), col("product_name"), col("rating"), col("review_date"),
        when(col("rating") >=4 , ("High"))
        .when(col("rating") >= 3 && col("rating") < 4, ("Medium"))
        .otherwise("Low").alias("Rating_Category"))
    val df2 = df1.show()

    //
    product_review.filter(col("product_name").startsWith("S")).show

    df1.groupBy("Rating_category").agg(count(col("rating")),avg(col("rating"))).show()

    scala.io.StdIn.readLine()

  }
}

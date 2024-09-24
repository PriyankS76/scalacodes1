import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object A2_BQ11 {
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



    val book_inventory = Seq(
      (1,"The Great Gatsbye",150, "2024-01-10"),
      (2,"The Catcher in the Rye",80, "2024-01-15"),
      (3,"Moby Dick",200,"2024-01-20"),
      (4,"To Kill a Mockingbird",30,"2024-02-01"),
      (5,"The Odyssey",60,"2024-02-10"),
      (6,"War and Peace",20,"2024-03-01"))
      .toDF("book_id","book_title", "stock_quantity" ,"last_updated")


    val df1 = book_inventory.select(col("book_id"), col("book_title"), col("stock_quantity"), col("last_updated"),
      when(col("stock_quantity") > 100 , ("High"))
        .when(col("stock_quantity") >= 50 && col("stock_quantity") < 100, ("Medium"))
        .otherwise("Low").alias("stock_level"))
    val df2 = df1.show()

    //
    df1.filter(col("book_title").startsWith("The")).show
    df1.groupBy("stock_level").agg(sum(col("stock_quantity")),avg(col("stock_quantity"))
      ,max(col("stock_quantity")),min(col("stock_quantity"))).show()


    scala.io.StdIn.readLine()

  }

}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, lag, year}

object A3_Q8 {
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

    val product_prices = List(
      ("P001", "Mobile Phone", "2023-10-01", 500, "Electronics"),
      ("P002", "Washing Machine", "2023-10-05", 700, "Home Appliances"),
      ("P003", "Laptop", "2023-10-10", 1200, "Electronics"),
      ("P004", "TV", "2023-10-15", 1000, "Consumer Electronics"),
      ("P005", "Mobile Phone", "2023-11-01", 550, "Electronics"),
      ("P006", "Laptop", "2023-11-10", 1250, "Electronics"),
      ("P007", "TV", "2023-11-15", 1050, "Consumer Electronics"),
      ("P008", "Mobile Phone", "2023-12-01", 600, "Electronics"),
      ("P009", "Laptop", "2023-12-10", 1300, "Electronics"),
      ("P010", "TV", "2023-12-15", 1100, "Consumer Electronics")
    ).toDF("product_id", "product_name", "price_date", "price", "category")
//
//    You have a DataFrame product_prices with columns: product_id, product_name, price_date, price,
//    and category.
//     Create a new column price_month using month extracted from price_date.
//     Filter records where category ends with 'Electronics' and the price has increased compared
//      to the previous month.
//

    val extmonth = product_prices.withColumn("price_month", year($"price_date"))
    extmonth.show()

    val df1 = Window.partitionBy("category").orderBy("price_month")
    val change_analysis = extmonth.withColumn("prev_month", lag("price", 1).over(df1))

    val df2 = change_analysis.withColumn("Change_price", col("price")-col("prev_month"))
    df2.show()

    val df3 = change_analysis.filter(col("category").endsWith("Electronics") && col("price")>col("prev_month"))
    df3.show()
//     Group by product_name and price_month, and calculate:
    //      o The average price per month.
    //    o The maximum price fluctuation (difference between max and min price) within the
    //    month.
    //      o The count of price changes.
    //     Use the lag function to calculate the price change percentage between consecutive months
    //    for each product
    //
    val df4 = df2.groupBy("product_name","price_month").agg(avg("price"),(functions.max("price")-functions.min("price")).alias("price diff")
      ,count("Change_price"))
    df4.show()

   val df5 = df2.withColumn("Percentage change", (col("Change_price")/col("price"))*100).show()

  }
}

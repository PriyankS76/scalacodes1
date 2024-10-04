import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}

object A3_Q12 {
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
    val product_stock = List(
      ("P001", "Mobile Phone", "Electronics", "2023-10-01", 80, "Supplier A"),
      ("P002", "Washing Machine", "Home Appliances", "2023-11-05", 120, "Supplier B"),
      ("P003", "Laptop", "Electronics", "2023-12-10", 60, "Supplier A"),
      ("P004", "TV", "Consumer Electronics", "2023-12-15", 95, "Supplier C"),
      ("P005", "Mobile Phone", "Electronics", "2024-01-01", 40, "Supplier A"),
      ("P006", "Tablet", "Electronics", "2024-01-10", 70, "Supplier B"),
      ("P007", "Camera", "Electronics", "2024-01-15", 30, "Supplier C"),
      ("P008", "Headphones", "Electronics", "2024-02-01", 50, "Supplier A"),
      ("P009", "Bluetooth Speaker", "Electronics", "2024-02-05", 20, "Supplier B"),
      ("P010", "Smart Watch", "Electronics", "2024-02-10", 90, "Supplier C"))
      .toDF("product_id","product_name","category","stock_date","stock_level","supplier")

//    You have a DataFrame product_stock with columns: product_id, product_name, category,
//    stock_date, stock_level, and supplier.
//     Create a new column stock_month using month extracted from stock_date.
//     Filter records where category starts with 'Electro' and stock_level is below 100.
//     Group by supplier and stock_month, and calculate:
//      o The average stock_level per month.
//    o The maximum stock_level for any product.
//    o The count of low stock occurrences (where stock_level < 50).
//     Use the lag function to track stock level changes for each product over time.

    val extmonth = product_stock.withColumn("stock_month", month($"stock_date"))
    extmonth.show()
    val df1 = extmonth.filter(col("category").startsWith("Electro") && col("stock_level")<100)
    df1.show()

    val df2 = extmonth.groupBy("supplier", "stock_month")
      .agg(sum("stock_level"), functions.max("stock_level"), sum(when(col("stock_level")< 50,1).otherwise(0)))
    df2.show()

    val df3 = Window.partitionBy("product_name").orderBy("stock_date")
    val df4 = extmonth.withColumn("prev_level", lag("stock_level", 1).over(df3))
//
    val df6 = df4.withColumn("Stock_change", col("stock_level") - col("prev_level")).show()
  }
}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, lag, sum, when}
import org.apache.spark.sql.{SparkSession, functions}

object A3_Q3 {
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


    val sales_target = List(
      ("S001", 15000, 12000, "2023-12-10", "North", "Electronics Accessories"),
      ("S001", 5000, 12000, "2023-12-10", "North", "Electronics Accessories"),
      ("S002", 8000, 9000, "2023-12-11", "South", "Home Appliances"),
      ("S003", 20000, 18000, "2023-12-12", "East", "Electronics Gadgets"),
      ("S004", 10000, 15000, "2023-12-13", "West", "Electronics Accessories"),
      ("S005", 18000, 15000, "2023-12-14", "North", "Furniture Accessories"))
      .toDF("salesperson_id", "sales_amount", "target_amount", "sale_date", "region", "product_category")
    //
    //
    val df1 = sales_target.select(col("salesperson_id"), col("sales_amount"), col("target_amount"),
      col("sale_date"), col("region"), col("product_category"),
      when(col("sales_amount") >= col("target_amount"), "achieved").otherwise("Not achieved").alias("Target"))
    df1.show()

    val df2 = df1.filter(col("product_category").startsWith("Electronics") && col("product_category").endsWith("Accessories")).show()

    val df3 = df1.groupBy("region", "product_category").agg(sum(col("sales_amount")).alias("sum of sales_amount"), functions.min(col("sales_amount"))).show()

    val df4 = df1.groupBy("Target").agg(count(when(col("Target") === "achieved",
      col("salesperson_id"))).alias("sales_person_count")).show()

    val sales_window = Window.partitionBy("salesperson_id").orderBy("sales_amount")
    val current_sales = df1.withColumn("current_sales", lag("sales_amount", 1)
      .over(sales_window))
    current_sales.show()

  }
}

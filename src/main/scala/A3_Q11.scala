import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}

object A3_Q11 {
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

    val customer_purchases = List(
      ("P001", "C001", "Running Shoes", "2023-10-10", 100, "Credit Card"),
      ("P002", "C002", "Basketball Shoes", "2023-11-01", 150, "Debit Card"),
      ("P003", "C003", "Soccer Shoes", "2023-12-01", 120, "Credit Card"),
      ("P004", "C001", "Dress Shoes", "2024-01-15", 200, "Credit Card"),
      ("P005", "C004", "Running Shoes", "2024-02-10", 110, "Cash"),
      ("P006", "C001", "Casual Shoes", "2024-02-15", 90, "Credit Card"),
      ("P007", "C002", "Sneakers", "2024-03-01", 130, "Credit Card"),
      ("P008", "C003", "Formal Shoes", "2024-03-05", 160, "Credit Card"),
      ("P009", "C001", "Sports Shoes", "2024-03-10", 115, "Credit Card"),
      ("P010", "C002", "Running Shoes", "2024-04-01", 140, "Debit Card")
    ).toDF("purchase_id", "customer_id", "product_name", "purchase_date", "purchase_amount", "payment_method")

    //    You have a DataFrame customer_purchases with columns: purchase_id, customer_id,
    //    product_name, purchase_date, purchase_amount, and payment_method.
    //     Create a new column purchase_month using month extracted from purchase_date.
    //     Filter records where product_name ends with 'Shoes' and payment_method starts with
    //    'Credit'.
    //     Group by customer_id and purchase_month, and calculate:
    //      o The total purchase_amount per customer per month.
    //    o The minimum purchase_amount in any purchase.
    //      o The count of purchases per month.
    //     Use the lead function to calculate the difference in purchase_amount between consecutive
    //      purchases for each customer

    val extmonth = customer_purchases.withColumn("purchase_month", month($"purchase_date"))
    extmonth.show()
    val df1 = extmonth.filter(col("product_name").endsWith("Shoes") && col("payment_method").startsWith("Credit"))
    df1.show()

    val df2 = extmonth.groupBy("customer_id", "purchase_month")
      .agg(sum("purchase_amount"), functions.min("purchase_amount"), count("purchase_id"))
    df2.show()

    val df4 = Window.partitionBy("customer_id").orderBy("purchase_date")
    val df5 = extmonth.withColumn("Next_amount", lead("purchase_amount", 1).over(df4))
    val df6 = df5.withColumn("Difference_amount", col("Next_amount") - col("purchase_amount")).show()


    //      val df4 = df3.functions.max(sum(col("status"))).over(Window.partitionBy("employee_id").orderBy("attendance_date")))
    //      df4.show()

  }
}
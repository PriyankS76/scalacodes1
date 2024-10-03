import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, lag, sum, year}

object A3_Q6 {
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

   val productReturns = List(
    ("R001", "O001", "Electro Gadgets", "2023-12-01", "Damaged", 100),
    ("R002", "O002", "Home Appliances", "2023-12-05", "Defective", 50),
     ("R003", "O003", "Electro Toys", "2023-12-10", "Changed Mind", 75),
     ("R004", "O004", "Electro Gadgets", "2023-12-15", "Damaged", 100),
     ("R005", "O005", "Kitchen Set", "2023-12-20", "Wrong Product", 120),
     ("R006", "O006", "Electro Speakers", "2023-12-21", "Damaged", 95),
     ("R007", "O007", "Home Appliances", "2023-12-25", "Defective", 60),
     ("R008", "O008", "Electro TV", "2024-01-01", "Changed Mind", 80),
     ("R009", "O009", "Electro Gadgets", "2024-01-05", "Damaged", 110),
    ("R010", "O010", "Kitchen Set", "2024-01-10", "Wrong Product", 130)
   ).toDF("return_id", "order_id", "product_name", "return_date", "return_reason", "refund_amount")

//     Create a new column return_year using year extracted from return_date.
//     Filter records where product_name starts with 'Electro' and the refund_amount is not null.
//     Group by return_reason and return_year, and calculate:
//      o The total refund_amount.
//    o The count of returns for each reason.
//    o The average refund_amount per return.
//     Use the lag function to compare each product's refund amount with its previous return

    val extyear = productReturns.withColumn("return_year", year($"return_date"))
    extyear.show()

    val df1 = extyear.filter(col("product_name").startsWith("Electro") && col("refund_amount").isNotNull)
    df1.show()
//
    val df2 = extyear.groupBy("return_reason","return_year").agg(sum("refund_amount"),avg("refund_amount"),count("return_id"))
    df2.show()
//
    val df3 = Window.partitionBy("product_name").orderBy("refund_amount")
    val change_analysis = extyear.withColumn("prev_returns", lag("refund_amount", 1).over(df3))
//
    val df4 = change_analysis.withColumn("Change", col("refund_amount")-col("prev_returns"))
    df4.show()


  }
}

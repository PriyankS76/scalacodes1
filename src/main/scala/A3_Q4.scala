import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, datediff, lead, sum}

object A3_Q4 {
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
//    You have a DataFrame loan_repayments with columns: loan_id, customer_id, repayment_amount,
//    due_date, payment_date, loan_type, and interest_rate.
//
//    Sample Data:
//      loan_id customer_id repayment_amount due_date payment_date loan_type interest_rate
//
    val loan_repayments = List(
      ("L001", "C001", 1000,"2023-11-01","2023-12-01", "Personal Loan",7.5),
      ("L002", "C002", 2000,"2023-11-15","2023-11-20", "Home Loan",6.5),
      ("L003", "C003", 1500,"2023-12-01","2023-12-25", "Personal Loan",8.0),
      ("L004", "C004", 2500,"2023-12-10","2024-01-15", "Car Loan",9.0),
      ("L005", "C005", 1200,"2023-12-15","2024-01-20", "Personal Loan",7.8),
      ("L005", "C005", 1500,"2023-12-15","2024-01-20", "Personal Loan",7.8))
      .toDF("loan_id", "customer_id", "repayment_amount", "due_date", "payment_date", "loan_type", "interest_rate")
    //
    //
//     Create a new column repayment_delay by calculating the difference in days between
    //    payment_date and due_date.
    //     Filter records where loan_type starts with 'Personal' and repayment_delay is greater than 30
    //    days.
    //     Group by loan_type and interest_rate, and calculate:
    //      o The total repayment_amount collected.
    //      o The maximum repayment_delay.
    //    o The average interest_rate for delayed payments.
    //     Use the lead function to predict the next repayment amount for each customer.

    val dateDiffDf = loan_repayments.withColumn("repayment_delay", datediff($"payment_date", $"due_date"))
      dateDiffDf.show()
    val df1 = dateDiffDf.filter(col("loan_type").startsWith("Personal") && col("repayment_delay") >= 30)
      df1.show()

    val df2 = dateDiffDf.groupBy("loan_type", "interest_rate").agg(sum(col("repayment_amount")).alias("sum of repayment_amount"),
             functions.max(col("repayment_delay")),
      avg(col("interest_rate"))).show()

    val loan_window = Window.partitionBy("customer_id").orderBy("repayment_amount")
        val current_sales = dateDiffDf.withColumn("next_repayment", lead("repayment_amount", 1)
          .over(loan_window))
        current_sales.show()

////
//
  }
}
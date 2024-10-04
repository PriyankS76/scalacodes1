import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, lag, sum, when, year}

object A3_Q13 {
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
    val bank_transactions = List(
      ("T001", "A001", "Withdrawal", "2023-11-01", 6000, "Branch X"),
      ("T002", "A002", "Deposit", "2023-11-05", 2000, "Branch Y"),
      ("T003", "A003", "Withdrawal", "2023-12-01", 5500, "Branch X"),
      ("T004", "A004", "Withdrawal", "2023-12-10", 7000, "Branch Z"),
      ("T005", "A005", "Deposit", "2024-01-15", 3000, "Branch Y"),
      ("T006", "A001", "Withdrawal", "2024-01-20", 8000, "Branch X"),
      ("T007", "A002", "Withdrawal", "2024-01-25", 5500, "Branch Y"),
      ("T008", "A003", "Deposit", "2024-02-01", 10000, "Branch X"),
      ("T009", "A004", "Withdrawal", "2024-02-05", 9000, "Branch Z"),
      ("T010", "A005", "Deposit", "2024-02-10", 2500, "Branch Y"))
      .toDF("transaction_id", "account_id", "transaction_type", "transaction_date", "transaction_amount", "branch")

    //    You have a DataFrame bank_transactions with columns: transaction_id, account_id,
    //    transaction_type, transaction_date, transaction_amount, and branch.
    //     Create a new column transaction_year using year extracted from transaction_date.
    //     Filter records where transaction_type starts with 'Withdrawal' and the transaction_amount
    //    is greater than 5000.
    //



    val extyear = bank_transactions.withColumn("transaction_year", year($"transaction_date"))
    extyear.show()
    val df1 = extyear.filter(col("transaction_type").startsWith("Withdrawal") && col("transaction_amount") > 5000)
    df1.show()

//     Group by branch and transaction_year, and calculate:
    //      o The total transaction_amount for withdrawals.
    //      o The average transaction_amount for deposits.
    //      o The count of transactions for each branch.
    val df2 = extyear.groupBy("branch", "transaction_year")
      .agg(sum(when(col("transaction_type") === "Withdrawal", col("transaction_amount"))
        .otherwise(0)).as("total_withdrawal_amount"),
        avg(when(col("transaction_type") === "Deposit", col("transaction_amount"))
          .otherwise(0)).as("total_withdrawal_amount"),
        count("transaction_id")).show()

    //     Use the lag function to analyze the trend in transaction_amount for each account over time
        val df3 = Window.partitionBy("account_id").orderBy("transaction_date")
        val df4 = extyear.withColumn("prev_amount", lag("transaction_amount", 1).over(df3))
        val df5 = df4.withColumn("transaction_trend", col("transaction_amount") - col("prev_amount")).show()
  }
}

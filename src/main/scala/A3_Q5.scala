import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, lag, lead, month, sum}

object A3_Q5 {
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

     val websiteTraffic = List(
      ("S001", "U001", 15, "2023-10-01", "Mobile", "Organic"),
      ("S002", "U002", 10, "2023-10-05", "Desktop", "Paid"),
      ("S003", "U003", 20, "2023-10-10", "Mobile", "Organic"),
      ("S004", "U004", 25, "2023-10-15", "Tablet", "Referral"),
      ("S005", "U001", 30, "2023-11-01", "Mobile", "Organic"),
      ("S006", "U003", 18, "2023-11-05", "Mobile", "Organic"),
       ("S006", "U003", 32, "2023-12-05", "Mobile", "Organic"),
      ("S007", "U005", 22, "2023-11-08", "Mobile", "Organic"),
      ("S008", "U006", 12, "2023-11-12", "Desktop", "Paid"),
      ("S009", "U007", 28, "2023-11-20", "Mobile", "Organic"),
      ("S010", "U002", 20, "2023-11-25", "Mobile", "Organic")
    ).toDF("session_id", "user_id", "page_views", "session_date", "device_type", "traffic_source")
//    You have a DataFrame website_traffic with columns: session_id, user_id, page_views, session_date,
//    device_type, and traffic_source.
//     Create a new column session_month using month extracted from session_date.
//     Filter records where traffic_source is 'Organic' and device_type is 'Mobile'.
//     Group by device_type and session_month, and calculate:
//      o The total page_views.
//    o The average page_views per session.
//    o The count of sessions.
//     Use the lead and lag functions to analyze the change in page_views between consecutive
//      sessions for each user.

    val extmonth = websiteTraffic.withColumn("session_month", month($"session_date"))
    extmonth.show()

    val df1 = extmonth.filter(col("traffic_source")=== "Organic" && col("device_type")=== "Mobile")
    df1.show()

    val df2 = extmonth.groupBy("device_type","session_month").agg(sum("page_views"),avg("page_views"),count("session_id"))
    df2.show()

    val df3 = Window.partitionBy("user_id").orderBy("page_views")
    val change_analysis = extmonth.withColumn("Next_views", lead("page_views", 1).over(df3))
      .withColumn("pre_views", lag("page_views", 1).over(df3))

      val df4 = change_analysis.withColumn("Change", col("Next_views")-col("pre_views"))
    df4.show()
   















    ////
    //
  }
}

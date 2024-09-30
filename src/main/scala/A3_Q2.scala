import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, lead, sum, when, year}

object A3_Q2 {
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

    val customer_churn = List(
      ("C001","Premium Gold","Yes","2023-02-01",1200,"USA"),
      ("C002","Basic","No","NULL",400,"Canada"),
      ("C003","Premium Silver","Yes","2023-11-15",800,"UK"),
      ("C004","Premium Gold","Yes","2023-01-10",1500,"USA"),
      ("C005","Basic","No","NULL",300,"India"))
      .toDF("customer_id", "subscription_type", "churn_status", "churn_date", "revenue", "country")

//    Create a new column churn_year using year extracted from churn_date.
//     Filter records where subscription_type starts with 'Premium' and churn_status is not null.
//     Group by country and churn_year, and calculate:
//      o The total revenue lost due to churn per year.
//    o The average revenue per churned customer.
//      The count of churned customers.
//     Use the lead function to find the next year's revenue trend for each country

    val extyear = customer_churn.withColumn("churn_year", year($"churn_date"))
    extyear.show()

    val df1 = extyear.filter(col("subscription_type").startsWith("Premium") && col("churn_status").isNotNull).show()

    val df2 = extyear.groupBy("country","churn_year").agg(sum(when(col("churn_status") === "Yes",col("revenue"))).alias("revenue lost"))
    df2.show()

    val df3 = extyear.groupBy("country","churn_year","customer_id").agg(avg(when(col("churn_status") === "Yes",col("revenue"))).alias("revenue lost"),
      count(when(col("churn_status") === "Yes",col("customer_id"))).alias("Count of consumers"))
    df3.show()

val cus_window = Window.partitionBy("country").orderBy("churn_year")
        val cus_lead = extyear.withColumn("nextyearrevenue", lead("revenue", 1)
          .over(cus_window))

        val rev_trend = cus_lead.withColumn("revenue_trend", col("nextyearrevenue") - col("revenue"))

        rev_trend.show()

  }
}

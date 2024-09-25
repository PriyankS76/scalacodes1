import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max, min, sum, when}

object A2_BQ17 {
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

    val project_budget = List(
      (1,"New Website",50000,55000),
      (2,"Old Software",30000,25000),
      (3,"New App",15000,15000),
      (4,"Old Campaign",20000,18000),
      (5,"New Research",60000,70000))
      .toDF("project_id", "project_name", "budget", "spent_amount")

     val df1 = project_budget.select(col("project_id"), col("project_name"), col("budget"), col("spent_amount"),
      when(col("spent_amount") > col("budget"), ("Over Budget"))
        .when(col("spent_amount") === col("budget"), ("On Budget"))
        .otherwise("Under Budget").alias("budget_status"))
    val df2 = df1.show()

    df1.filter(col("project_name").startsWith("New")).show()

    df1.groupBy("budget_status").agg(sum(col("spent_amount")),avg(col("spent_amount"))
      ,max(col("spent_amount")),min(col("spent_amount"))).show()

    scala.io.StdIn.readLine()
  }
}

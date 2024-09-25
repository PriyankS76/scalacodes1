import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max, min, sum, when}

object A2_BQ13 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "Priyank")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._


    val salaries = Seq(
      (1,"IT",130000, "2024-01-10"),
      (2,"HR",80000, "2024-01-15"),
      (3,"IT",60000, "2024-02-20"),
      (4,"IT",70000, "2024-02-25"),
      (5,"Sales",50000, "2024-02-20"),
      (6,"IT",90000, "2024-02-20"))
      .toDF("employee_id","department", "salary" ,"last_increment_date")

    val df1 = salaries.select(col("employee_id"), col("department"), col("salary"), col("last_increment_date"),
      when(col("salary") > 120000 , ("High"))
        .when(col("salary") >= 60000 && col("salary") < 120000, ("Medium"))
        .otherwise("Low").alias("salary_band"))
    val df2 = df1.show()

    //
    df1.filter(col("department").startsWith("IT")).show
    df1.groupBy("salary_band").agg(sum(col("salary")),avg(col("salary"))
      ,max(col("salary")),min(col("salary"))).show()


    scala.io.StdIn.readLine()

  }
}

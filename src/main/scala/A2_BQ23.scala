import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max, min, sum, when}

object A2_BQ23 {
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

    val training_records = Seq(
      (1,1,50, "Tech"),
      (2,2,25, "Tech"),
      (3,3,15, "Management"),
      (4,4,35, "Tech"),
      (5,5,45, "Tech"),
      (6,6,30, "HR"))
      .toDF("record_id","employee_id","training_hours", "training_type")

    val df1 = training_records.select(col("record_id"), col("employee_id"), col("training_hours"), col("training_type"),
      when(col("training_hours") > 40 , ("Extensive"))
        .when(col("training_hours") >= 20 && col("training_hours") < 40, ("Moderate"))
        .otherwise("Minimal").alias("training_status"))
    val df2 = df1.show()

    //
    df1.filter(col("training_type").startsWith("Tech")).show
    df1.groupBy("training_status").agg(sum(col("training_hours")),avg(col("training_hours"))
      ,max(col("training_hours")),min(col("training_hours"))).show()


    scala.io.StdIn.readLine()

  }
}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object A2_BQ19 {
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

    val support_tickets = List(
      (1,"Bug",1.5,"High"),
      (2,"Feature",3.0,"Medium"),
      (3,"Bug",4.5,"Low"),
      (4,"Bug",2.0,"High"),
      (5,"Enhancement",1.0,"Medium"),
      (6,"Bug",5.0,"Low"))
      .toDF("ticket_id", "issue_type", "res_time", "priority")


    val df1 = support_tickets.select(col("ticket_id"), col("issue_type"), col("res_time"), col("priority"),
      when(col("res_time") <=2, ("Quick"))
        .when(col("res_time") > 2 && col("res_time") < 4, ("Moderate"))
        .otherwise("Slow").alias("Res_status"))
    df1.show()

    df1.filter(col("issue_type").contains("Bug")).show()
    df1.groupBy("Res_status").agg(sum(col("res_time")),avg(col("res_time")),max(col("res_time")),min(col("res_time"))).show()
    scala.io.StdIn.readLine()
  }
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object A1_SQ2 {
  def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder()
    .appName("Priyank")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._ // for toDF function

  val data=List((1,85),(2,42),(3,73)).toDF("student_id","score") //toDF provides column name

  data.select(col("student_id"),col("score"),when(col("score")>=50,("Pass")).otherwise("Fail").alias("Result")).show()

}
}

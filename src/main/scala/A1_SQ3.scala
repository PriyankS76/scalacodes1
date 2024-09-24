import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object A1_SQ3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Priyank")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function

    val data=List((1,1000),(2,200),(3,5000)).toDF("transaction_id","amount") //toDF provides column name


    data.select(col("transaction_id"),col("amount"),when(col("amount")>=1000,("High")).when(col("amount")>=500 && col("amount")< 1000,("Medium")).otherwise("Low").alias("Result")).show()

  }
}

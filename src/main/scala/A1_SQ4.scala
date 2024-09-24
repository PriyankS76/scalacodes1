import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object A1_SQ4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Priyank")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function

    val data=List((1,30.5),(2,150.75),(3,75.25)).toDF("product_id","price") //toDF provides column name


    data.select(col("product_id"),col("price"),when(col("price")<50,("Cheap")).when(col("price")>=50 && col("price") <100,("Moderate")).otherwise("Expensive").alias("Price_Range")).show()

  }
}

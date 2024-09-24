import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object A1_SQ1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Priyank")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._ // for toDF function

    val data=List((1,"John",28),(2,"Jane",35),(3,"Doe",22)).toDF("id","name","age") //toDF provides column name

    data.select(col("id"),col("name"),col("age"),when(col("age")>=18,("Adult")).otherwise("Not adult").alias("Is adult")).show()

  }
}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession, functions => F}

object A2_Q2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "karthik")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val scoreData = Seq(
      (1,"Smartphone",700, "Electronics"),
      (2,"TV",1200, "Electronics"),
      (3,"Shoes",150,"Apparel"),
      (4,"Socks",150,"Apparel"),
      (5,"Laptop", 800, "Electronics"),
      (6,"Jacket",200,"Apparel"))
    .toDF("product_id","product_name", "Price" ,"Category")

    scoreData.select(col("product_id"), col("product_name"), col("Price"), col("Category"),
      when(col("price") > 500, ("Expensive"))
        .when(col("price") >= 200 && col("price") < 500, ("Moderate"))
        .otherwise("Cheap").alias("Price_Category")).show()

//    scoreData.createOrReplaceTempView("customer")
//
//    spark.sql("""
//    SELECT product_id,
//           product_name,
//           Price,
//           Category
//
//           CASE
//               WHEN Price > 500 THEN 'Expensive'
//               WHEN Price BETWEEN 200 AND 500 THEN 'Moderate'
//               ELSE 'Cheap'
//           END
//    FROM customer
//""").show()

    scoreData.filter(col("product_name").startsWith("S")).show
    scoreData.filter(col("product_name").endsWith("s")).show



    scoreData.groupBy("Category").agg(sum("Price") as "Sum Price").show()
    scoreData.groupBy("Category").agg(avg("Price") as "Avg Price").show()
    scoreData.groupBy("Category").agg(max("Price") as "Max Price").show()
    scoreData.groupBy("Category").agg(min("Price") as "Min Price").show()




    scala.io.StdIn.readLine()

  }
}

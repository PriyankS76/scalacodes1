import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession, functions => F}

object A2_Q3 {
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

    val empData = List(
      (1, "John", 28, 60000),
      (2, "Jane", 32, 75000),
      (3, "Mike", 45, 120000),
      (4, "Alice", 55, 90000),
      (5, "Steve", 62, 110000),
      (6, "Clair", 40, 40000))
      .toDF("Employee_id", "name", "age", "salary")

    val df1 = empData.select(col("Employee_id"), col("name"), col("age"), col("salary"),
      when(col("age") < 30, ("Young"))
        .when(col("age") >= 30 && col("age") < 50, ("Mid"))
        .otherwise("Senior").alias("Age_group"))

      df1.show()


//    val df2 = df1.select(col("Employee_id"), col("name"), col("age"), col("Salary"),col("Age_group"),
//      when(col("salary") > 100000, ("High"))
//        .when(col("salary") >= 50000 && col("salary") < 100000, ("Medium"))
//        .otherwise("Low").alias("Salary_range")).show()

    df1.createOrReplaceTempView("customer")

    //using sparkSQL
    spark.sql("""
    SELECT
         Employee_id,name,age,
         salary,
         Age_group,
         case
         when salary > 100000 then "High"
         when salary between 50000 and 100000 then "Medium"
         else "Low"
         end as Salary_range
         FROM customer
""").show()

    spark.sql("""
    SELECT * FROM customer where name like  "J%"
""").show()

    spark.sql("""
    SELECT
         *
    FROM customer where name like "%e"
""").show()


    spark.sql("""
    SELECT
         Age_group,
         sum(salary),
         avg(salary),
         max(salary),
         min(salary)
    FROM customer group by Age_group
""").show()

//    empData.filter(col("name").startsWith("J")).show
//    empData.filter(col("name").endsWith("e")).show
//
//    df1.groupBy("Age_group").agg(sum(col("salary")),avg(col("salary")),max(col("salary")),min(col("salary"))).show()
////    df1.groupBy("Age_group").agg(sum("salary") as "Sum salary").show()
////    df1.groupBy("Age_group").agg(avg("salary") as "Avg salary").show()
//    df1.groupBy("Age_group").agg(max("salary") as "Max salary").show()
//    df1.groupBy("Age_group").agg(min("salary") as "Min salary").show()


    scala.io.StdIn.readLine()
  }
}

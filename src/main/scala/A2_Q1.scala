import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object A2_Q1 {
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
      (1,"Alice",92, "Math"),
      (2,"Bob",85, "Math"),
      (3,"Carol",77,"Science"),
      (4,"Dave",65,"Science"),
      (5,"Eve",50, "Math"),
      (6,"Frank",82,"Science")
    ).toDF("Student_id","name", "Score" ,"Subject")

    scoreData.select(col("Student_id"), col("name"), col("Subject"), col("Score"),
      when(col("score") >= 90, ("A"))
        .when(col("score") >= 80 && col("score") < 90, ("B"))
         .when(col("score") >= 70 && col("score") < 80, ("C"))
         .when(col("score") >= 60 && col("score") < 70, ("D"))
       .otherwise("E").alias("Grade")).show()


       scoreData.groupBy("Subject").agg(avg("score") as "Avg Score").show()
       scoreData.groupBy("Subject").agg(max("score") as "Max Score").show()
       scoreData.groupBy("Subject").agg(min("score") as "Min Score").show()

      

    scala.io.StdIn.readLine()
  }
}


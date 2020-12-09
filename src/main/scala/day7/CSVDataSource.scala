package day7

import org.apache.spark.sql.SparkSession

object CSVDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("csvDatasource")
      .master("local[2]")
      .getOrCreate()
    val csv = spark.read.csv("F:\\IDEA\\maven_workplace\\Spark\\csv")
    csv.printSchema()

    val pdf = csv.toDF("id","name","age")

    pdf.show()
    spark.stop()
  }

}

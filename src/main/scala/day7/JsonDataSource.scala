package day7

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 2020/12/9
 * sparkSessiond读取json类型数据
 */
object JsonDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("jsonDaaSource")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._
    val jsons: DataFrame = spark.read.json("F:\\IDEA\\maven_workplace\\Spark\\json")
    val filtered = jsons.where($"age" <= 400)

    filtered.printSchema()
    filtered.show()

    spark.stop()
  }
}

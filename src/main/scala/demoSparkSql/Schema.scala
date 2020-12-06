package demoSparkSql

import org.apache.spark.sql.SparkSession

/**
 * 使用反射推断Schema
 * Spark SQL 的 Scala 接口支持自动转换一个包含 case classes 的 RDD 为 DataFrame。
 * Case class 定义了表的 Schema。Case class 的参数名使用反射读取并且成为了列名
 */
object Schema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("schema")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val peopleDF = spark.sparkContext
      .textFile(args(1))
      .map(_.split(","))
      .map(atr => Person(atr(0),atr(1).trim.toInt) )
      .toDF()
    peopleDF.createOrReplaceTempView("people")
    val teenagers = spark.sql("SELECT name ,age FROM people WHERE age BETWEEN 13 AND 19")
    teenagers.map(teenager => "Name:"+teenager(0)).show()
  }
}

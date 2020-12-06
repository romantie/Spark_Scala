package demoSparkSql

import org.apache.spark.sql.SparkSession

object DataFrames {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("sparkSql")
      .master("local[2]")
      .getOrCreate()

    val df = spark.read.json(args(0))
    //df.show()
    //df.printSchema()
    //df.select("name").show()
    //df.select($"name",$"age"+1).show()
    //df.filter($"age">21).show()
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.show()
    import spark.implicits._
    val caseClassDf = Seq(Person("lmk",22)).toDS()
    caseClassDf.show()

  }
}

case class Person(str: String, i: Int)

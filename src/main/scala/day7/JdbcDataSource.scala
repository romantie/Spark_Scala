package day7

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 2020/12/6
 * spark读取jdbc数据源，并将修改数据写入数据库
 */
object JdbcDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("jdbcSource")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._
    val logs: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/bigdata",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "people",
        "user" -> "root",
        "password" -> "root")
    ).load()

    logs.printSchema()
    logs.show()

    //使用spark的lambda表达式筛选数据
    val ages = logs.filter($"age" <= 46)
    ages.show()

    val result = ages.select($"id",$"name",$"age"*10 as "age")
    //将年龄乘10并保存到新的数据库中
    val props = new Properties()
    props.put("user","root")
    props.put("password","root")
    result.write.mode("ignore").jdbc("jdbc:mysql://localhost/bigdata","people2",props)
    result.show()

    spark.close()
  }
}

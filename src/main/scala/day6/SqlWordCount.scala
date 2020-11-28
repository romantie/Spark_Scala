package day6

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
 * 2020/11/2
 * 时间过得真特娘的快，今天一定搞定sparksql
 */
object SqlWordCount {
  def main (args:Array[String]): Unit = {
    //创建sparkSession
    val spark = SparkSession.builder()
      .appName("SqlWordcont")
      .master("local[3]")
      .getOrCreate()

    //指定读数据的路径
    //dataset 分布式数据集，是对rdd的进一步封装，是更加智能的rdd
    //dataset 只有一列，默认这列叫value
    val lines: Dataset[String] = spark.read.textFile("F:\\IDEA\\maven_workplace\\Spark\\teacher.log")
    //lines.show()

    //整理数据，切分压平，
    //导入隐式转换
    import spark.implicits._
    //val words: Dataset[String] = lines.flatMap(line => line.split("/"))
    val words: Dataset[(String, String, String)] = lines.map(line => {
      val fields = line.split("/")
      val ht = fields(0)
      val teacher = fields(3)
      val subject = fields(2).split("[.]")(0)
      (ht, teacher, subject)
    })


    words.show()
    //注册视图
    words.createTempView("v_wordCount")
    //执行sql
    val results: DataFrame = spark.sql("SELECT _3,COUNT(*) counts FROM v_wordCount GROUP BY _3 ORDER BY counts DESC")
    results.show()
    spark.stop()
  }
}

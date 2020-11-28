package day7

import day4.MyUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 妈耶，时间咋这么快，多看看写过的项目，不能写完就忘啊
 * 2020/11/2
 * 使用spark重写第三天的ip查找，
 */
object IpLocationSQL {

  def main (args:Array[String]) :Unit ={

    val spark = SparkSession
      .builder()
      .master("local[5]")
      .appName("ipsql")
      .getOrCreate()
    //读取到文件中的ip规则
    import spark.implicits._
    val rulesLines: Dataset[String] =spark.read.textFile(args(0))
    //整理规则
    val rulesDataFrame: DataFrame = rulesLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    }).toDF("snum", "enum", "province")
    rulesDataFrame.createTempView("v_rules")

    //创建rdd。读取访问日志
    val accessLines: Dataset[String] = spark.read.textFile(args(1))
    //整理数据
    val ipDataFrame: DataFrame = accessLines.map(log => {
      //将日志的行切分
      val fields = log.split("[|]")
      val ip = fields(1)
      val ipNum = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_Num")
    ipDataFrame.createTempView("v_ips")

    rulesDataFrame.show()
    ipDataFrame.show()
    val result: DataFrame = spark.sql("SELECT province,count(*) counts FROM v_ips JOIN v_rules ON (ip_Num >= snum AND ip_Num<=enum) GROUP BY province ORDER BY counts DESC" )
    result.show()

    spark.stop()

  }
}

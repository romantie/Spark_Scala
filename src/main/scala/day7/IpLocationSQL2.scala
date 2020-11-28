package day7

import day4.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 裂开撒，任务完不成l
 * 在上个sparksql中，join的代价大而且慢，
 * 解决思路，将表缓存起来，广播变量
 */
object IpLocationSQL2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ipsql2")
      .master("local[3]")
      .getOrCreate()

    //将文件中的数据读入
    import spark.implicits._
    val rulesLines: Dataset[String] = spark.read.textFile(args(0))
    val rulesDataset: DataFrame = rulesLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum,endNum,province)
    }).toDF("snum", "enum", "province")

    //收集ip规则到driver端
    val rulesDriver = rulesDataset.collect()
    //广播必须使用sparkcontext
    //将广播变量的引用返回到Driver端
    val broadcastRef: Broadcast[DataFrame] = spark.sparkContext.broadcast(rulesDataset)

    //创建rdd，读取访问日志
    val accessLines: Dataset[String] = spark.read.textFile(args(1))

    //整理数据
    val ipDataFrame: DataFrame = accessLines.map(log => {
      //将日志的每一行切分
      val fields = log.split("[|]")
      val ip = fields(1)
      //将ip转换成十进制
      val ipNUm = MyUtils.ip2Long(ip)
      ipNUm
    }).toDF("ip_num")
    ipDataFrame.createTempView("v_log")






  }

}

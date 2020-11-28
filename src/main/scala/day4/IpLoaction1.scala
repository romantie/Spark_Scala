package day4

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLoaction1 {

  def main(args: Array[String]): Unit = {
    //配置spark
    val conf = new SparkConf().setAppName("IP1").setMaster("local[3]")
    val sc = new SparkContext(conf)

    //再drive端获取全部的数据，（全部的数据都再同以他机器）
    val rules = MyUtils.readRules(args(0))

    //再Drive端的数据广播到Executor
    //调用sc的广播方法，广播引用
    val broadcast: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)

    //创建rdd，读取访问日志
    val accessLines = sc.textFile(args(1))
    //再Drive端定义函数方便调用
    val func = (line:String )=>{
      val fields = line.split("[|]")
      val ip = fields(1)
      //将ip转化为十进制
      val ipNum = MyUtils.ip2Long(ip)
      //用二分法查找，通过Drive端的引用或者取到executor中的广播变量
      //（该函数中的代码是在Executor中别调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播的规则了）
      val rulesInExecutor: Array[(Long, Long, String)] = broadcast.value

      val index = MyUtils.binarySearch(rulesInExecutor,ipNum)
      var province = "unknow"
      if(index != -1){
        province=rulesInExecutor(index)._3
      }
      (province,3)
    }
    //整理数据
    val provinceAndOne: RDD[(String, Int)] = accessLines.map(func)
    //聚合
    val reduceProvice = provinceAndOne.reduceByKey(_+_)

    //将结果打印
    val r = reduceProvice.collect()
    println(r.toBuffer)

    sc.stop()



  }

}

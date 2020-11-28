package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * created by lmk on 2020/10/16
 * 按视频敲出来的单词计数
 */
object ScalaWordCount {

  def main(args: Array[String]): Unit = {
    //创建spark配置，设置应用程序名字
    val conf=new SparkConf().setAppName("WordCCount").setMaster("local[2]")
    //创建spark的入kou
    val sc=new SparkContext(conf)
    //指定以后从哪里读取数据创建rdd
    val lines: RDD[String] =sc.textFile("F:/testlm.txt")
    //切分压平
    val words: RDD[String] =lines.flatMap(_.split(" "))
    //将单词和1组合

    val wordAndOne: RDD[(String, Int)] = {
      words.map((_,1))
    }
    //按key值进行聚合
    val reduce: RDD[(String, Int)] =wordAndOne.reduceByKey(_+_)
    val reduceColl: Array[(String, Int)] =reduce.collect()
    //排序
    val sorted: RDD[(String, Int)] =reduce.sortBy(_._2,false)
    //将结果保存
    //sorted.saveAsTextFile("F:/testreslmk")
    println("执行完毕")
    println(reduceColl.toBuffer)
    //释放资源
    sc.stop()

  }
}

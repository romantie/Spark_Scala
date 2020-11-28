package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * created by lmk on 2020/10/24
 * 程序员节快乐，学习了几天spark，自己独立敲一个spark单词统计程序吧
 */
object WordCountSelf {
  def main(args: Array[String]): Unit = {
    //配置sprk
    val conf: SparkConf = new SparkConf().setAppName("wordcountSelf").setMaster("local[3]")
    val sc: SparkContext = new SparkContext(conf)
    //读取本地的文本数据
    val lines: RDD[String] = sc.textFile("F:/IDEA/maven_workplace/Spark/teacher.log")
    //整理数据,将单词切分压平
    val word: RDD[String] = lines.flatMap(_.split("/"))
    //将单词和1组合，组成键值对
    val wordAndOne ={
      word.map((_,1))
    }
    //聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    val sorted = reduced.sortBy(_._2,false)
    val result = sorted.collect()
    println(result.toBuffer)
    sc.stop()
  }
}

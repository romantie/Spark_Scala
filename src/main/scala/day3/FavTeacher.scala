package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher {

  def main(args: Array[String]): Unit = {

    //配置spark的参数
    val conf = new SparkConf().setAppName("favteacher").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //读取数据
    val lines: RDD[String] = sc.textFile("F:/IDEA/maven_workplace/Spark/teacher.log")
    //println(lines.collect)

    //整理数据
    val teacherAndOne = lines.map(line =>{

      val teacher = line.split("/")(3)
      (teacher,1)
    })
    //聚合
    val reduce: RDD[(String, Int)] =teacherAndOne.reduceByKey(_+_)
    //排序
    val sorted = reduce.sortBy(_._2,false)
    //出发action计算
    val result = sorted.collect()

    //打印
    println(result.toBuffer)

    sc.stop()

  }
}

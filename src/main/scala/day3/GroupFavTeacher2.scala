package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * creat 2020/10/23 lmk
 * 将不同学科最受欢迎的老师找出，先分组在排序
 */
object GroupFavTeacher2 {
  def main(args: Array[String]): Unit = {
    //配置spark
    val conf = new SparkConf().setAppName("gfteacher2").setMaster("local[3]")
    val sc = new SparkContext(conf)
    //读取数据
    val lines = sc.textFile("F:/IDEA/maven_workplace/teacher.log")
    //整理数据
    val SubAndTeacher = lines.map(line =>{

      val teacher = line.split("/")(3)
      val sub = line.split("/")(2).split("[.]")(0)
      ((sub,teacher),1)

    })
    //聚合
    val reduce: RDD[((String, String), Int)] = SubAndTeacher.reduceByKey(_+_)

    //定义一个数组存放学科
    val sbs = Array("javaee","php","bigdata")
    val topN = sbs.length
    //过滤分组排序
    for (sb<-sbs){
      val filtered: RDD[((String, String), Int)] = reduce.filter(_._1._1==sb)
      val favTeachers = filtered.sortBy(_._2,false).take(topN)
      println(favTeachers.toBuffer)
    }
    //val sorted = reduce.filter(_._1._1=="bigdata").sortBy(_._2)
    //val fav = sorted.collect()
    //println(fav.toBuffer)
  sc.stop()



  }

}

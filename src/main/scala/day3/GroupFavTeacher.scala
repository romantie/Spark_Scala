package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * created 2020/10/23 lmk
 * 将最受欢迎的老师找出，先保存到数组，在进行排序
 */
object GroupFavTeacher {

  def main(args: Array[String]): Unit = {
    //配置spark
    val conf = new SparkConf().setAppName("gfavteacher").setMaster("local[2]")
    val sc=new SparkContext(conf)

    //读取数据
    val lines = sc.textFile("F:/IDEA/maven_workplace/teacher.log")
    //整理数据
    val TeacherAndSub = lines.map(line => {

        val teacher = line.split("/")(3)
        val sub = line.split("/")(2).split("[.]")(0)
      ((sub,teacher),1)
    })
    //聚合，将[学科，老师],相同的聚合起来
    val reduce: RDD[((String, String), Int)] = TeacherAndSub.reduceByKey(_+_)
    //分组，按照学科进行分组
    val group: RDD[(String, Iterable[((String, String), Int)])] = reduce.groupBy(_._1._1)

    //排序
    val sorted: RDD[(String, List[((String, String), Int)])] = group.mapValues(_.toList.sortBy(_._2).reverse.take(3))

    val result: Array[(String, List[((String, String), Int)])] = sorted.collect()
    println(result.toBuffer)
    sc.stop()




  }
}

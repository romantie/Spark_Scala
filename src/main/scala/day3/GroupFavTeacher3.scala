package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
 * created by 2020/10/24 lmk
 * 最受欢迎老师第三版，将所有数据拉分区，然后在分区内进行排序
 */
object GroupFavTeacher3 {

  def main(args: Array[String]): Unit = {

    //配置spark
    val conf = new SparkConf().setAppName("favteacher3").setMaster("local[3]")
    val sc = new SparkContext(conf)
    //读取本地文件数据
    val lines = sc.textFile("F:/IDEA/maven_workplace/teacher.log")

    //整理数据
    val SubAndTeacher = lines.map(line => {
      //提取老师和学科的数据
      val teacher = line.split("/")(3)
      val sub = line.split("/")(2).split("[.]")(0)
      ((sub,teacher),1)

    })

    //聚合，统计每个学科老师出现的次数
    val reduced: RDD[((String, String), Int)] = SubAndTeacher.reduceByKey(_+_)
    //计算学科的数量
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()

    //自定义个分区器，并且按照指定的分区器进行分区
    val sbPartitioner = new SubjectPatitioner(subjects)

    //partitionBy按照指定的分区规则进行分区
    //调用partitionBy时RDD的Key是(String, String)
    val partitoned = reduced.partitionBy(sbPartitioner)

    //一次拿出一个分区并且进行排序
    val sorted = partitoned.mapPartitions(it =>{
      it.toList.sortBy(_._2).reverse.take(3).iterator
    })

    val result = sorted.collect()
    println(result.toBuffer)

    sc.stop()
  }

}
//自定义的分区器
case class SubjectPatitioner(subjects: Array[String]) extends Partitioner{

  //相当于主构造器（new的时候回执一次）
  //用于存放规则的一个map
  val rules = new mutable.HashMap[String,Int]()
  var i = 0
  for (sb<-subjects){
    rules.put(sb,i)
    i += 1
  }
  //返回分区的数量（下一个RDD有多少分区）
  override def numPartitions: Int = subjects.length

  //根据传入的key计算分区标号
  //key是一个元组（String， String）
  override def getPartition(key: Any): Int = {
    //获取传入的学科名称
    val subject = key.asInstanceOf[(String,Int)]._1
    //根据规则计算分区编号
    rules(subject)
  }
}
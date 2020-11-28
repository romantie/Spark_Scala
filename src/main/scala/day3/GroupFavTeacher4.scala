package day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable
/**
 * creatd by lmk on 2020/10/24
 * 1024节日快乐
 */
object GroupFavTeacher4 {

  def main(args: Array[String]): Unit = {
    //配置saprk
    val conf = new SparkConf().setAppName("gft4").setMaster("local[3]")
    val sc = new SparkContext(conf)
    //读取本地文件的数据
    val lines: RDD[String] = sc.textFile("F:/IDEA/maven_workplace/teacher.log")
    //将学科和老师（sub，teacher）聚合，
    val SubAndTeacher: RDD[((String, String), Int)] = lines.map (line => {
      //获取老师和学科
      val teacher = line.split("/")(3)
      val sub = line.split("/")(2).split("[.]")(0)
      ((sub, teacher), 1)
    })

    //计算有多少个学科
    val subjects: Array[String] = SubAndTeacher.map(_._1._1).distinct().collect()

    //自定义一个分区器，并按照指定的分区器进行分区
    val sbPartitoner: SubjectPartiton = new SubjectPartiton(subjects)

    //按照学科和老师聚合，改rdd中一个分区只有一个学科
    val reduced: RDD[((String, String), Int)] = SubAndTeacher.reduceByKey(sbPartitoner,_+_)
    //如此一次拿出一个分区，操作一个分区的数据
    val sorted: RDD[((String, String), Int)] = reduced.mapPartitions(it=>{
      //将迭代器转换为list，然后排序，在转换成迭代器返回
      it.toList.sortBy(_._2).reverse.iterator
    })
    val result: Array[((String, String), Int)] = sorted.collect()
    println(result.toBuffer)

    sc.stop()



  }

}

case class SubjectPartiton(subjects: Array[String]) extends Partitioner{
  //用于存放规则的map
  val rules = new mutable.HashMap[String,Int]()
  var i = 0
  for (sb<-subjects){
    rules.put(sb,i)
    i+=1
  }

  override def numPartitions: Int = subjects.length

  //根据传入的key计算分区号
  //key传入的是一个元组（String，String）
  override def getPartition(key: Any): Int = {
    val subject = key.asInstanceOf[(String,String)]._1
      //计算分区编号
    rules(subject)
  }
}
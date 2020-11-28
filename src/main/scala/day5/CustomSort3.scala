package day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * creatd by lmk 2020/10/31
 */
object CustomSort3 {

  def main(args: Array[String]): Unit = {
    //配置spark
    val conf = new SparkConf().setAppName("sort3").setMaster("local[3]")
    val sc = new SparkContext(conf)

    //排序规则：首先按照颜值的降序，如果颜值相等，再按照年龄的升序
    val users= Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")
    //并行化变成rdd
    val lines = sc.parallelize(users)
    //切分整理数据
    val tpRDD = lines.map(line =>{
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt
      (name,age,fv)
    })
    //排序，传入一个排序规则
    //val sorted: RDD[(String, Int, Int)] = tpRDD.sortBy(tp => XIANROU(tp._2,tp._3))
    val sorted: RDD[(String, Int, Int)] = tpRDD.sortBy(tp=>(-tp._3,tp._2))
    print(sorted.collect().toBuffer)
    sc.stop()

  }

}

//case class XIANROU(age: Int, fv: Int)

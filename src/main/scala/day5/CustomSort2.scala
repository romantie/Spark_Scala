package day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * created by lmk on 2020/10/30
 */
object CustomSort2 {
  def main(args: Array[String]): Unit = {
    //配置spark程序
    val conf = new SparkConf().setAppName("sorted2").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val users= Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")

    //使用sc并行化rdd序列
    val lines: RDD[String] = sc.parallelize(users)
    //z整理数据
    val tpRdd = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt
      (name,age,fv)
    })
    val sortd: RDD[(String, Int, Int)] = tpRdd.sortBy(tp => new Boy(tp._2,tp._3))
    val result = sortd.collect()

    println(result.toBuffer)

    sc.stop()

  }
}

case class Boy(age: Int, fv: Int) extends Ordered[Boy] with Serializable {
  override def compare(that: Boy): Int = {
    if (this.fv == that.fv){
      this.age - that.age
    }else {
      -(this.fv-that.fv)
    }

  }

 // override def toString: String = s",age:$age,fv:$fv "

}

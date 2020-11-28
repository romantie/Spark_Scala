package day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 2020/10/30
 */
object CustomSort {
  def main(args: Array[String]): Unit = {
    //配置spark
    val conf = new SparkConf().setAppName("sorted1").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val users= Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")

    //调用sc接口实现,并行化rdd序列
    val lines: RDD[String] = sc.parallelize(users)
    //z整理数据
    val userRDD = lines.map(line=> {
      //切分
      val fields: Array[String] = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt
      //(name,age,fv)
      new User(name, age, fv)
    })
    //userRDD.sortBy(_.fv),不能满足要求
    val sorted = userRDD.sortBy(u => u)

    val r = sorted.collect()
    println(r.toBuffer)
    sc.stop()

  }

}

class User(val name: String, val age: Int, val fv: Int) extends Ordered[User] with Serializable{
  override def compare(that: _root_.day5.User): Int = {
    if(this.fv == that.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }

  override def toString: String = s"name: $name, age: $age, fv: $fv"

}




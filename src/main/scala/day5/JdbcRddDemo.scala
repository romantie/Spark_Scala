package day5

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * created by lmk 2020/10/31
 * SPARK使用接口读取数据库中的数据
 */
object JdbcRddDemo {

  val getConn =()=> {
    //DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEnconding=utf-8","root","root")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?", "root", "root")

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkJdbc").setMaster("local[3]")
    val sc = new SparkContext(conf)

      //创建rdd，从MySQL数据库擢内阁读取数据
    val jdbcRDD = new JdbcRDD(
      sc,
      getConnection = getConn,
      sql = "SELECT * FROM people WHERE id >= ? AND id<?",
      lowerBound = 1,
      upperBound = 4,
      numPartitions = 1,
      mapRow = rs => {
        val id = rs.getInt(1)
        val name = rs.getString(2)
        val age = rs.getInt(3)
        (id, name, age)
      }
    )

    //触发Action
    val rs = jdbcRDD.collect()
    println(rs.toBuffer)

    sc.stop()
  }

}

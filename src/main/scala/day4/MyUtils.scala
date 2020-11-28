package day4


import scala.io.{BufferedSource, Source}
import scala.tools.nsc.io.Path

object MyUtils {

  def ip2Long(ip: String): Long={
    val fragment = ip.split("[.]")
    var ipNum = 0L
    for (i<- 0 until fragment.length){
      ipNum = fragment(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def readRules(path: String):Array[(Long,Long,String)]={
    //读取ip规则m,读入本地数据
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理
    val rules: Array[(Long, Long, String)] = lines.map(line =>{
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum,endNum,province)
    }).toArray
    rules
  }


    def binarySearch(lines:Array[(Long,Long,String)],ip:Long): Int={
      var low = 0
      var high = lines.length-1
      while(low<=high){
        val mid = (low+high)/2
        if ((ip >= lines(mid)._1) && ip <= lines(mid)._2)
          return mid
        if (ip < lines(mid)._1)
          high = mid - 1
        else{
          low = mid + 1
        }
      }
      -1

    }

  def main(args: Array[String]): Unit = {
    val ip = "125.213.100.123"
    val iptol = ip2Long(ip)
    println(iptol)
    val rules: Array[(Long, Long, String)] = readRules("F:/IDEA/maven_workplace/Spark/ip/ip.txt")
    println(rules.toBuffer.take(10))
    println(rules.length)

    val index = binarySearch(rules,iptol)

    println(index)
    val tp = rules(index)
    println(tp._3)

  }
}



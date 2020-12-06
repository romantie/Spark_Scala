package day7

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 2020/12/6对不起我是憨批，小半个月没学spark了
 * 今天搞定sparksql和连接hive
 * 测试spark SQL的内连接和外连接
 */
object JoinText {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("jointest")
      .master("local[3]")
      .getOrCreate()
    //构造一份数据，包含id，姓名，国家的英文名
    import spark.implicits._
    val lines: Dataset[String] = spark.createDataset(List("1,laozhao,china","1,laoyang,usa"))

    //对数据进行整理
    val tpDs = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val nationCode = fields(2)
      (id, name, nationCode)
    })
    val df1 = tpDs.toDF("id","name","enation")

    //构造第二份数据，国家的英文名和中文名的对应关系
    val nations = spark.createDataset(List("china,中国","usa,美国"))
    val ndataset = nations.map(line => {
      val fields = line.split(",")
      val ename = fields(0)
      val cname = fields(1)
      (ename, cname)
    })
    val df2 = ndataset.toDF("ename","cname")
    df2.count()

    //第一种是创建视图
    df1.createTempView("v_users")
    df2.createTempView("v_nations")
    val result = spark.sql("SELECT * FROM v_users JOIN v_nations ON enation = ename")
    result.show()

    //第二种方式，使用dataset的api
    val result2 = df1.join(df2,$"enation" === $"ename","left_outer" )
    result2.show()

    //关闭spark
    spark.stop()
  }
}

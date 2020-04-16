package cn.postgres

import org.apache.spark.sql.SparkSession

/**
  *
  * author: LG 
  * date: 2019-06-13 15:52
  * desc:
  *
  */
object PostgresReader {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("PostgresReader").master("local").getOrCreate()

    val pgUrl = "jdbc:postgresql://192.168.44.46:5432/lg_home"
    // 读取pg中的数据
    val df1 = spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://192.168.44.46:5432/lg_home")
      .option("user", "postgres")
      .option("password", "199771")
      .option("dbtable", "gg")
      .load()
    df1.show()
  }

}

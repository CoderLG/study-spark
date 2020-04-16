package cn.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  *
  * author: LG 
  * date: 2019-06-17 09:07
  * desc:
  *
  */
object Short {

  def main(args: Array[String]): Unit = {
    //val spark = SparkSession.builder().appName("short").getOrCreate()
    val conf = new SparkConf().setAppName("Short")
    val sc = new SparkContext(conf)

  }
}

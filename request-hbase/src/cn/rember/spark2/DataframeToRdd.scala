package cn.rember.spark2

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object DataframeToRdd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("nn").getOrCreate()
    import spark.sqlContext.implicits._
    val arr = Array((1,2,3),(4,5,6))
    val rdd = spark.sparkContext.parallelize(arr)
    val frame = rdd.toDF("id","sex","name")
    var arrB  =ArrayBuffer()
    val value= frame.rdd.map(r => {
      println( r.get( r.fieldIndex("id")))
    })
    value.count()

    spark.stop()
  }
}

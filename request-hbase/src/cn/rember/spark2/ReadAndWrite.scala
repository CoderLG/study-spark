package cn.rember.spark2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  *
  * author: LG 
  * date: 2019-07-04 16:13
  * desc:
  *
  */
object ReadAndWrite {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("nn")
      .config("spark.sql.hive.convertMetastoreParquet",true)
      .config("spark.sql.parquet.compression.codec","gzip")
      .getOrCreate()



    import spark.implicits._

    val arr = Array((1,2,3),(4,5,6))
    val rdd: RDD[(Int, Int, Int)] = spark.sparkContext.parallelize(arr)
    rdd.saveAsTextFile("dataset")

    val frame: DataFrame = rdd.toDF("id","sex","name")
    frame.show()
    frame.printSchema()
    frame.distinct()
      .repartition(2)
      .write.mode(SaveMode.Overwrite).format("parquet")
      .save("gggg")

    /*  val frame = spark.read.format("parquet").load("ggg")
    frame.show()
    val schema: StructType = frame.schema*/

  }
}

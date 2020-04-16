package cn.hbase

import java.{lang, util}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions
import scala.collection.JavaConverters._
/**
  *
  * author: LG 
  * date: 2019-06-14 14:08
  * desc:
  *
  */
object HbaseReader {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("readHbase")
      .master("local")
      .getOrCreate()

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","did")
    conf.set("hbase.zookeeper.property.clientPort","2181")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "wf_test")
    val hbase: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hbase.collect.map(x=>{

    println(x._2)
    })

 /*   spark.sqlContext
      .read
      .options(Map("hbase_table_name"->"wf_test"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()*/




//    tuples.map(t => {
//
//      print(t._2.getRow)
//
//    })
/*    val df = hbase.map(r=>{
      r._2.getMap().forEach( (k1,v1)=>{
        print("k1:")
        println(Bytes.toString(k1));
        v1.forEach((k2,v2)=>{
          print("k2:")
          println(Bytes.toString(k2))
        })
      })*/

      //      (
      //      Bytes.toString(r._2.getValue(Bytes.toBytes("irisfeature"),Bytes.toBytes("feature1"))),
      //      Bytes.toString(r._2.getValue(Bytes.toBytes("irisfeature"),Bytes.toBytes("feature2"))),
      //      Bytes.toString(r._2.getValue(Bytes.toBytes("irisfeature"),Bytes.toBytes("feature3"))),
      //      Bytes.toString(r._2.getValue(Bytes.toBytes("irisfeature"),Bytes.toBytes("feature4"))),
      //      Bytes.toString(r._2.getValue(Bytes.toBytes("irisclass"),Bytes.toBytes("class")))
      //    )
   // })

    //val count = df.count()
    //println("Students RDD Count:" + count)

  }

}

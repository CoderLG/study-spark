package cn.rember.spark1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, _}

object Spark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("day day up")
    val sc = new SparkContext(conf)
    val  sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    //创建Rdd
    val tuples = Array((1,"a","b"),(1,"b","c"),(3,"bb","cc"))
    val rdd: RDD[(Int, String, String)] = sc.parallelize(tuples)
    val tuples2 = Array((1,"a","b"),(2,"b","c"),(3,"bb","cc"))
    val rdd2 = sc.parallelize(tuples2)
    val frame = rdd.toDF("id","name","sex")
    val frame2 = rdd.toDF("id1","name1","sex")

    //创建Rdd
    val structType =StructType(frame.schema .+: (StructField("add",LongType,true)))
    val value =  frame.rdd.zipWithIndex().map(el=>{
      val t = el._2 +: el._1.toSeq
      Row.fromSeq(t)
    })
    //val res=sqlContext.createDataFrame(res,structType)
    //res.show()

    //val res2 = frame.union(frame2)
    //res2.show()

    //getAs 是从0开始的
    val res3 = frame.groupBy("id").count().map(row => {row.getAs[Int](0)})
    //res3.show()

    //列名
    val rows: String = frame.columns.mkString(",")
    //rows.foreach(println(_))

    val column = frame.col("name")
    val res4 = frame.withColumn("aa",column)
   // res4.show()

    val res5: RDD[WrappedArray[String]] = frame.select("id","name").rdd.map(line => line.getAs[WrappedArray[String]](0))
    //res5.foreach(println(_))

    //getResultSet(sqlContext).show(false)

    val first: (Int, String, String) = rdd.first()
    val row: Row = frame.rdd.first()
    //println(frame.rdd.first().length + 1)


    var map = Map[String,String]()
    map += ("id1" -> "StringType")

    //frame.show()
    var struct: StructType = new StructType()
    frame.schema.foreach(sf=>{
      val name = sf.name
      val dataType = map.getOrElse(sf.name,"00").toString
      if(dataType.equals("00")){
        struct = struct.add(sf.name,sf.dataType,sf.nullable)
      }else{
        val tmpSF = dataType match {
          case "IntegerType" => struct = struct.add(StructField(name,IntegerType))
          case "DoubleType" =>  struct = struct.add(StructField(name,DoubleType))
          case "StringType" =>  struct = struct.add(StructField(name,StringType))
          case _ =>  struct = struct.add(sf)
        }
      }
    })

    val bcMap = sc.broadcast(map)
    val columns = frame.columns
    val newRdd: RDD[Row] = frame.rdd.map(row => {
      var arr = ArrayBuffer[Any]()
      (0 until row.length).foreach(x=>{
        val typ = bcMap.value.getOrElse(columns(x),"00")
        if(typ.equals("00")){
          arr += row.get(x)
        }else{
          typ match {
            case "IntegerType" =>  arr += row.get(x).toString.toInt
            case "DoubleType" =>   arr += row.get(x).toString.toDouble
            case "StringType" =>   arr += row.get(x).toString
            case _ =>   arr += row.get(x)
          }
        }
      })
     Row.fromSeq(arr)
    })

    val ff = sqlContext.createDataFrame(newRdd,struct)
   // ff.show()
   // println(sc.version)


    sc.stop()
  }

  def getResultSet(sqlContext:SQLContext): DataFrame ={

    var registerLocation= scala.collection.mutable.Map[String,Int]()
    registerLocation.put("北京市",110000)
    registerLocation.put("市辖区", 110100)
    registerLocation.put("东城区", 110101)
    registerLocation.put("西城区", 110102)
    registerLocation.put("崇文区", 110103)
    var i= -1
    val unit = registerLocation.map(x=>{
      i += 1
      (i,(x._1,x._2))
    }).toMap


    val doubleMap = scala.collection.mutable.Map[Int,Double]()
    doubleMap.put(0,1.0)
    doubleMap.put(1,2.0)
    doubleMap.put(2,3.0)
    doubleMap.put(3,4.0)
    val arrb = ArrayBuffer[resultSet]()
    for(x <- (0 until 1000)){
      val tmp = doubleMap(scala.util.Random.nextInt(2))
      val tmpStr = if(tmp == 1.0) "男" else "女"
      val tmp2 = doubleMap(scala.util.Random.nextInt(2))
      val tmp2Str = if(tmp2 == 1.0) "有" else "无"
      val tmp3 = doubleMap(scala.util.Random.nextInt(2))
      val tmp3Str = if(tmp3 == 1.0) "有" else "无"

      arrb +=  resultSet(
        getID(),
        doubleMap(scala.util.Random.nextInt(4)),
        doubleMap(scala.util.Random.nextInt(4)),"迁入原因",
        tmp,tmpStr,
        doubleMap(scala.util.Random.nextInt(4)),"关系",
        doubleMap(scala.util.Random.nextInt(4)),"户口类别",
        tmp2,tmp2Str,
        getYMD, doubleMap(scala.util.Random.nextInt(4)),
        tmp3,tmp3Str,
        doubleMap(scala.util.Random.nextInt(2)))
    }
    val frame11 = sqlContext.createDataFrame(arrb)
    return frame11
  }


  case class resultSet(
                        身份证号:String,
                        户籍区划:Double,
                        迁入原因编码:Double,
                        迁入原因:String,
                        性别编码:Double,
                        性别:String,
                        与户主关系编码:Double,
                        与户主关系:String,
                        户口类别编码:Double,
                        户口类别:String,
                        有无户籍编码:Double,
                        有无户籍:String,
                        迁入时间:String,
                        民族:Double,
                        有无配偶编号:Double,
                        有无配偶:String,
                        是否犯罪:Double
                      )

  def getYMD(): String ={
    val year = scala.util.Random.nextInt(38)+1880
    val month = scala.util.Random.nextInt(12)+1
    val day = scala.util.Random.nextInt(28)+1
    return year.toString.trim + "-" + month.toString.trim + "-" + day.toString.trim
  }
  def getID(): String ={
    var registerLocation= scala.collection.mutable.Map[String,Int]()
    registerLocation.put("0",110000)
    registerLocation.put("1", 110100)
    registerLocation.put("2", 110101)
    registerLocation.put("3", 110102)
    registerLocation.put("4", 110103)
    registerLocation.put("5", 110104)
    registerLocation.put("6", 110105)
    registerLocation.put("7", 110106)
    registerLocation.put("8", 110107)
    registerLocation.put("9", 110108)
    registerLocation.put("10", 110109)
    registerLocation.put("11", 110111)
    registerLocation.put("12", 110112)
    registerLocation.put("13", 110113)
    registerLocation.put("14", 110114)
    registerLocation.put("15", 110115)
    registerLocation.put("16", 110116)
    registerLocation.put("17", 110117)
    registerLocation.put("18", 110200)
    registerLocation.put("19", 110228)

    registerLocation.put("20", 2412)
    registerLocation.put("21", 2654)
    registerLocation.put("22", 9875)
    registerLocation.put("23", 8524)
    registerLocation.put("24", 4589)
    registerLocation.put("25", 3456)
    registerLocation.put("26", 7867)
    registerLocation.put("27", 3457)
    registerLocation.put("28", 7893)
    registerLocation.put("29", 2487)
    registerLocation.put("30", 4634)
    registerLocation.put("31", 2567)
    registerLocation.put("32", 2438)
    registerLocation.put("33", 5678)
    registerLocation.put("34", 8098)
    registerLocation.put("35", 2346)
    registerLocation.put("36", 3457)
    registerLocation.put("37", 2543)
    registerLocation.put("38", 7896)
    registerLocation.put("39", 6645)

    val start = registerLocation.get(scala.util.Random.nextInt(20).toString)
    val startRes = start.mkString("")

    val yearRes = scala.util.Random.nextInt(38)+1880
    val month = scala.util.Random.nextInt(12)+1
    val monthRes =  if(month < 10)  0.toString + month.toString else month.toString

    val day = scala.util.Random.nextInt(28)+1
    var dayRes = if(day < 10) 0.toString + day.toString else day.toString

    val end =registerLocation.get((scala.util.Random.nextInt(20) + 20).toString)
    val endRes = end.mkString("")

    return startRes.trim + yearRes.toString.trim + monthRes.toString.trim + dayRes.toString.trim + endRes.toString.trim
  }
}

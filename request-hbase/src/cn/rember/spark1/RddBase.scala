package cn.rember.spark1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object RddBase {
  def main(args : Array[String]){

    accumulatorT
//    broadcast
//    countByKey

//    reduceT
//    countT
//    takeT
//    sortWordCount
//    wordNum
//    filter
//    groupByKey
//    join
//    cogroup
  }
  
  def cogroup(){
    val conf = new SparkConf()
        .setAppName("")
        .setMaster("local")
        
    val sc = new SparkContext(conf)
    val studentList = Array(
  		Tuple2(1,"leo"),
  		Tuple2(2,"jack"),
  		Tuple2(3,"mm"),
  		Tuple2(4,"ll"),
  	  Tuple2(5,"qq"),
  		Tuple2(6,"ww")
  		)
		
		val scoreList = Array(
			Tuple2(1,50),
			Tuple2(2,60),
			Tuple2(3,30),
			Tuple2(4,80),
			Tuple2(5,90),
			Tuple2(6,100),
			Tuple2(1,50),
			Tuple2(2,60),
			Tuple2(3,30),
			Tuple2(4,80),
			Tuple2(5,90),
			Tuple2(6,100)
				)
		
		val students = sc.parallelize(studentList)
	  val scores = sc.parallelize(scoreList)
	  students.cogroup(scores).foreach(
	      
	    q=> {
	      println(q._1 + " "+ q._2)
	    }
	  )
    
  }
  def join(){
    val conf = new SparkConf()
          .setAppName("")
          .setMaster("local")
          
    val sc = new SparkContext(conf)
    val studentList = Array(
			Tuple2(1,"leo"),
			Tuple2(2,"jack"),
			Tuple2(3,"mm"),
			Tuple2(4,"ll"),
		  Tuple2(5,"qq"),
			Tuple2(6,"ww")
			)
	
		val scoreList = Array(
			Tuple2(1,50),
			Tuple2(2,60),
			Tuple2(3,30),
			Tuple2(4,80),
			Tuple2(5,90),
			Tuple2(6,100),
			Tuple2(1,50),
			Tuple2(2,60),
			Tuple2(3,30),
			Tuple2(4,80),
			Tuple2(5,90),
			Tuple2(6,100)
				)
		
		val students = sc.parallelize(studentList)
	  val scores = sc.parallelize(scoreList)
	  
	  students.join(scores).foreach(
	    q =>{
	      println(q._1 + " "+ q._2)
	      
	    }    
	  )
    
  }
  def groupByKey(){
      val conf = new SparkConf()
          .setAppName("groupByKey")
          .setMaster("local")
          
      val sc = new SparkContext(conf)
      val scoreList = Array(
          Tuple2("class01",80),
          Tuple2("class02",70),
          Tuple2("class03",66),
          Tuple2("class01",23),
          Tuple2("class03",34),
          Tuple2("class04",86)
      )
      val classScore = sc.parallelize(scoreList)
      val res = classScore.groupByKey()
      res.foreach(
        r=>{
          print(r._1 + ":")
          r._2.foreach(
            w=>print(w + " ")
          )      
          println("\n-----")
        }    
      )
     
  }
  def filter(){
    
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)
    
    val numArr = Array(1,2,3,4,5,6)
    val numRdd = sc.parallelize(numArr)
    
    numRdd.filter(_ % 2 == 0).foreach(println(_))
   
  }
  
  def wordNum(){
    
    val conf = new SparkConf().setAppName("").setMaster("local[3]")
    val sc = new SparkContext(conf)
    
    val myFile = sc.textFile("E:/Hadoop/datas/WCInput.txt")
    
    val num = myFile.map(
        line => line.split(" ").length
   ).reduce(_ + _)
    
    println(num)
  }
  def sortWordCount(){
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)
    
    val myFile = sc.textFile("E:/WorkSpace/Home/data/WCInput.txt",1)
    
    val words = myFile.flatMap(
      _.split(" ")
    )
    
    val wordNum = words.map((_,1))
    val midRes = wordNum.reduceByKey(_ + _)
    val numWord = midRes.map(
        q=> (q._2,q._1)
     )
     
    val sortNumWord = numWord.sortByKey(false)
    sortNumWord.map(
        q=> (q._2,q._1)
     ).take(3).foreach(
       q => println(q._1 + " "+ q._2)
     )
     
    
  }
  def broadcast(){
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)
    
    var temp = 3
    val broad = sc.broadcast(temp)
    
    val numArr = Array(1,2,3,4,5,6)
    val numAdd = sc.parallelize(numArr)
    
    numAdd.map( _*broad.value).foreach(
      println(_)    
    )
    
    
  }

  def countByKey(){
     val conf = new SparkConf()
          .setAppName("")
          .setMaster("local")
          
      val sc = new SparkContext(conf)
      val studentArr = Array(
          Tuple2("class05","yy"),
          Tuple2("class01","leo"),
          Tuple2("class02","jack"),
          Tuple2("class03","mm"),
          Tuple2("class01","o"),
          Tuple2("class03","m"),
          Tuple2("class04","g")
      )
      
      val studentRDD = sc.parallelize(studentArr)
      val studentCount = studentRDD.countByKey()
      
      println(studentCount)
      
     
    
  }
  def takeT(){
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)
    
    val numArr = Array(1,2,3,4,5,6)
    val numRdd = sc.parallelize(numArr)
    
    val res = numArr.map(_*2).take(3)
    println(res.toBuffer)
    
    numRdd.map( _*2).take(2).foreach( println(_))
   
    
  }
  def countT(){
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)
    
    val numArr = Array(1,2,3,4,5,6)
    val numRdd = sc.parallelize(numArr, 1)
    
    val c1 = numRdd.count()
    println(c1)
//    val c2 = numArr.count()
    
  }
  
  
  def reduceT(){
    val conf = new SparkConf().setAppName("reduceT").setMaster("local")
    val sc = new SparkContext(conf)
    
    val numArr = Array(1,2,3,4,5,6)
    val numRdd: RDD[Int] = sc.parallelize(numArr, 1)
    
    val sum = numRdd.reduce(_ + _)
    println(sum)
    
    val ss = numArr.reduce(_ + _)
    println(ss)
    
  }
  
  def accumulatorT(){
    val conf = new SparkConf().setAppName("accumulatorT").setMaster("local")
    val sc = new SparkContext(conf)
    
    val numArr = Array(1,2,3,4,5,6)
    val numRdd = sc.parallelize(numArr, 1)
    val accumulatorNum = sc.longAccumulator
    
    numArr.foreach(q => accumulatorNum.add(q));
    println(accumulatorNum)
    numRdd.foreach(q => accumulatorNum.add(q))
    println(accumulatorNum)
    
  }
  
  
 
}
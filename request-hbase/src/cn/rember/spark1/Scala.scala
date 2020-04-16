package cn.rember.spark1

object Scala {
  def main(args: Array[String]): Unit = {

    val map1: Map[String, Double] = Map(("1" -> 2.0),("2" -> 3.0),("3"-> 4.0))
    val str: String = map1.mkString("\t")
//    println(str)

    val arr = Array.fill[Double](10)(1.0/10)
//   arr.map(println(_))
    val validatedModels: Array[String] = "svm,bayes,gbdt,rf,lr".split(",")


//    println(2.96.formatted("%.0f").toString)
//    println(2.96.formatted("%.1f").toString)

//    println(2.4.round)
//    println(2.5.round)
//    println(-2.4.round)
//    println(-2.5.round)

//    println(2.4.floor)
//    println(2.5.floor)
//    println(-2.4.floor)
//    println(-2.5.floor)

//    println(2.4.ceil)
//    println(2.5.ceil)
//    println(-2.4.ceil)
//    println(-2.5.ceil)

//    println("2".toDouble)




  }
}

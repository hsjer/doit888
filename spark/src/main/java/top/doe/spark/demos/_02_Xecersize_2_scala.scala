//package top.doe.spark.demos
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//object _02_Xecersize_2_scala {
//
//
//  def main(args: Array[String]): Unit = {
//
//    val conf = new SparkConf()
//    conf.setAppName("xx");
//    conf.setMaster("local");
//
//
//    // SparkContext sc = new SparkContext(conf);
//    val sc = new SparkContext(conf)
//
//    val rdd1 = sc.parallelize(List("a,1", "b,1", "a,10", "b,20", "a,30"))
//
//    val rdd2: RDD[(String, Int)] = rdd1.map(x => (x.split(",")(0), x.split(",")(1).toInt))
//
//    val res = rdd2.reduceByKey(_+_)
//
//    res.foreach(println)
//
//  }
//
//}

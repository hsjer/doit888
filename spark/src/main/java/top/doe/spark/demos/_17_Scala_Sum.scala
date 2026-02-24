//package top.doe.spark.demos
//
//import com.alibaba.fastjson.JSON
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.beans.BeanProperty
//
//
//case class OrderBean(uid:Int,oid:String,pid:Int,amt:Double)
//
//object _17_Scala_Sum {
//
//  def main(args: Array[String]): Unit = {
//
//    val conf = new SparkConf()
//    conf.setAppName("xxx")
//    conf.setMaster("local")
//
//    val sc = new SparkContext(conf)
//
//    val rdd1 = sc.textFile("spark_data/excersize_3/input/order.data")
//    val rdd2: RDD[OrderBean] = rdd1.map(s => JSON.parseObject(s,classOf[OrderBean]))
//
//    rdd2.map(od=>od.amt)
//      .sum()  // 隐式转换成DoubleRDD
//
//  }
//}

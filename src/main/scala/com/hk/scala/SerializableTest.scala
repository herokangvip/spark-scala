package com.hk.scala

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 各转换算子api
 */
object SerializableTest {
  def main(args: Array[String]): Unit = {
    //本地测试使用local模式，内存不够用需要设置testingMemory，线上不用
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("wordCount")
    conf.set("spark.testing.memory", "536870912") //后面的值大于512m即可
    val sc: SparkContext = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.makeRDD(1 to 20,4)
    println("分区数："+rdd1.getNumPartitions)
    val rules: MyRules = new MyRules()
    val resRdd = rdd1.mapPartitionsWithIndex((index,data) => {
      var ruleMap = rules.rulesMap
      val i: Int = ruleMap.getOrElse("hero", 0)
      val id: Long = Thread.currentThread().getId
      println(s"分区：$index，线程id $id,对象:$ruleMap")
      data.map((_, "分区号：" + index))
    })
    println(resRdd.collect().toBuffer)
    sc.stop()
  }
}
class MyRules extends Serializable {
  val rulesMap: Map[String, Int] = Map("hero" -> 1, "king" -> 2)
}
object MyRules2{
  val rulesMap: Map[String, Int] = Map("hero" -> 1, "king" -> 2)
}
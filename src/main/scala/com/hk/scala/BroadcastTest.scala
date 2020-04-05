package com.hk.scala

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * 各转换算子api
 */
object BroadcastTest {
  def main(args: Array[String]): Unit = {

    //本地测试使用local模式，内存不够用需要设置testingMemory，线上不用
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("wordCount")
    conf.set("spark.testing.memory", "536870912") //后面的值大于512m即可


    val sc: SparkContext = new SparkContext(conf)

    val rdd23: RDD[Int] = sc.makeRDD(1 to 9)
    var broad: User = new User
    //广播变量,如果是从hdfs读取需要将executor的数据collect到driver端合并，再广播到executor
    val broadcastRef: Broadcast[User] = sc.broadcast(broad)
    val rdd24: RDD[Int] = rdd23.map(d => {
      //Executor中获取广播变量
      val executorStr: User = broadcastRef.value
      println(executorStr)
      d * 10
    })
    rdd24.collect()

    sc.stop()

  }

  class User() extends Serializable

}

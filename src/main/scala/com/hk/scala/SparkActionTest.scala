package com.hk.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 行动算子
 */
object SparkActionTest {

  def main(args: Array[String]): Unit = {
    //本地测试使用local模式，内存不够用需要设置testingMemory，线上不用
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("wordCount")
    conf.set("spark.testing.memory", "536870912") //后面的值大于512m即可


    val sc: SparkContext = new SparkContext(conf)

    //从集合读取
    val rdd: RDD[Int] = sc.parallelize(1 to 3)

    val count: Long = rdd.count()
    println("============count:" + count)
    val first: Int = rdd.first()
    println("==============first:" + first)

    //取前n个元素
    val take: Array[Int] = rdd.take(2)
    println("=============take:" + take)

    //排序后前n个元素
    val ta: Array[Int] = rdd.takeOrdered(2)
    ta.foreach(println)

    //和aggregateBykey区别，1：一个是单值一个操作kv，2：aggregate如果有多个分区的话分区间计算时也会加上初始值，byKey只有分区内计算才计算初始值
    val i: Int = rdd.aggregate(0)(_ + _, _ + _)
    //fold类似agg

    //countByKey和count类似，区别是返回map
  }

}

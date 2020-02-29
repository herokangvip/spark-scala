package com.hk.scala

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * 各转换算子api
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    //本地测试使用local模式，内存不够用需要设置testingMemory，线上不用
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("wordCount")
    conf.set("spark.testing.memory", "536870912") //后面的值大于512m即可


    val sc: SparkContext = new SparkContext(conf)

    //从集合读取
    val rdd1: RDD[Int] = sc.parallelize(1 to 3)
    val rdd2: RDD[Int] = sc.makeRDD(1 to 9)
    val rdd3: RDD[String] = sc.makeRDD(List("a,b", "c,d"))


    //也可以从hdfs读取"hdfs://hadoop02:9000/input"
    val source: RDD[String] = sc.textFile("in")
    //val value2: RDD[(LongWritable, Text)] = sc.hadoopFile[LongWritable, Text, TextInputFormat]("hdfs://hadoop02:9000/input")
    val result: Array[(String, Int)] = source.flatMap(data => data.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()

    //print(result)//spark这样打印没有结果
    result.foreach(println)


    /*
    算子
    */
    //map
    //每个元素发送到excutor
    val v1: RDD[Int] = rdd1.map(_ * 2)
    //效率更高，减少数据发送到excutor的交互次数（只发送分区个次数），缺点所有数据都会发送，有可能oom
    val v2: RDD[Int] = rdd2.mapPartitions(data => {
      data.map(data => data * 2)
    })
    val v3: RDD[(Int, String)] = rdd2.mapPartitionsWithIndex((num, data) => {
      data.map((_, "分区号：" + num))
    })

    //flatMap
    val v4: RDD[String] = rdd3.flatMap(x => {
      x.split(",")
    })
    v4.collect().foreach(println)

    //glom,将每一个分区的数据放到数组
    val v5: RDD[Array[Int]] = rdd1.glom()
    v5.collect().foreach(d => {
      d.mkString
    })

    //分组奇偶数，（1，[1,3,5]）,（0，[2,4,6]）
    println("=======groupBy")
    val rdd6: RDD[Int] = sc.makeRDD(1 to 9)
    val v6: RDD[(Int, Iterable[Int])] = rdd6.groupBy(_ % 2)
    v6.collect.foreach(println)

    //filter
    println("=======filter")
    val rdd7: RDD[Int] = sc.makeRDD(1 to 11)
    val v7: RDD[(Int, Iterable[Int])] = rdd7.filter(d => d < 10) groupBy (_ % 2)
    v7.collect.foreach(println)


    //distinct，去重
    println("=======distinct")
    val rdd8: RDD[Int] = sc.makeRDD(1 to 11)
    //可以改变分区数
    val v8 = rdd8.distinct(1)
    v8.collect.foreach(println)

    //coalesce [ˌkoʊəˈles] 合并,1-16四个分区，会把3/4分区合并成一个，没有shuffle过程，100个变98个可以用（相差不大而且是大变小）
    //repartition 会有数据shuffle的过程（可以大变小也可以小变大）
    val rdd9: RDD[Int] = sc.makeRDD(1 to 16)
    val rdd10: RDD[Int] = sc.makeRDD(1 to 16)
    val v9 = rdd9.coalesce(3)
    val v10 = rdd10.repartition(2)
    v9.collect.foreach(println)

    //sortBy
    //union合并两个rdd
    //subtract，两个rdd的差集
    //intersection，两个RDD的交集
    //cartesian，笛卡尔积一般不使用


    //partitionBy
    //kv类型分区器，一个stage的任务数取决于最后一个rdd的分区个数
    //重要
    val rdd11 = sc.parallelize(Array((1, "aa"), (2, "bb")))
    val value: RDD[(Int, String)] = rdd11.partitionBy(new HashPartitioner(2))
    val value1: RDD[(Int, Iterable[(Int, String)])] = rdd11.groupBy(_._1)

    //reduceByKey(func: (V, V) => V)
    val rdd12 = sc.parallelize(Array((1, "aa"), (2, "bb")))
    val value2: RDD[(Int, String)] = rdd12.reduceByKey((data, data2) => {
      data + data2
    })

    //aggregateByKey,先分区内聚合，再分区间聚合
    val rdd13 = sc.parallelize(Array(("a", 1), ("b", 1), ("b", 1)))
    //柯里化+运行时类型推断
    val value13: RDD[(String, Int)] = rdd13.aggregateByKey(0)((a, b) => {
      a + b
    }, _ + _)

    //foldByKey,与aggregateByKey区别：分区内分区间使用相同的函数
    val rdd14 = sc.parallelize(Array(("a", 1), ("b", 1), ("b", 1)))
    val value14: RDD[(String, Int)] = rdd14.foldByKey(0)(_ + _)

    //,combineByKey与foldByKey、aggregateByKey区别：初始值函数和分区内分区间函数都可以自定义
    val rdd15 = sc.parallelize(Array(("a", 1), ("b", 1), ("b", 1)))
    val value15: RDD[(String, Int)] = rdd15.combineByKey(x => x, (d1: Int, d2) => {
      d1 + d2
    }, (d1: Int, d2: Int) => {
      d1 + d2
    })


    //orderByKey,参数true升序，false降序
    val rdd16 = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val value16: RDD[(Int, String)] = rdd16.sortByKey(true)


    //mapValues，和map区别只对k-v的v做处理
    val rdd17 = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val value17: RDD[(Int, String)] = rdd17.mapValues(v => v + "--")
    //结果：(1, "a--"), (2, "b--"), (3, "c--")


    //join，可以连接使用，双方的key必须匹配上
    val rdd18 = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd19 = sc.parallelize(Array((1, 99), (2, 99), (3, 99),(4, 99)))
    val value19: RDD[(Int, (String, Int))] = rdd18.join(rdd19)
    //出参:Array((1, ("a"，99）), (2, （"b",99）), (3, （"c",99）))
    //cogroup,与join区别，一：tuple中的数据包装成了可迭代对象，二:不匹配的键数据也会放过来比如上面例子中的4




  }

}

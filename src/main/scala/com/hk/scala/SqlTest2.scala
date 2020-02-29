package com.hk.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession}

/**
 * 自定义UDF函数
 */
object SqlTest2 {
  def main(args: Array[String]): Unit = {
    //
    val conf = new SparkConf().setMaster("local[1]").setAppName("sql")
    conf.set("spark.testing.memory", "536870912") //后面的值大于512m即可
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Word Count")
      .config(conf)
      .getOrCreate()

    //进行rdd和dataFrame和dataSet的转换需要引入隐式转换，
    //这里的spark是指sparkSession对象不是包名
    import spark.implicits._

    val sc: SparkContext = spark.sparkContext
    //创建rdd
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("zhangsan", 10), ("lisi", 20)))

    //rdd缓存,可以保存在内存、磁盘、堆外内存，触发action时不会重新计算，而是从缓存取，两个rdd之间插入一个缓存rdd
    //缓存不会立即触发，而是触发action时才会被缓存，persist底层也是cache方法，是以序列化的形式缓存在jvm堆空间中
    rdd.cache()
    //设置检查点保存目录
    sc.setCheckpointDir("hdfs:///input")
    //检查点会打断rdd的血缘关系
    rdd.checkpoint()

    //rdd转dataSet
    val ds: Dataset[User] = rdd.map(data => {
      User(data._1, data._2)
    }).toDS()

    //各种转换
    val df: DataFrame = rdd.toDF("name", "age")
    val ds2: Dataset[User] = df.as[User]
    val frame: DataFrame = ds2.toDF()
    val rdd1: RDD[User] = ds2.rdd
    val rdd2: RDD[Row] = frame.rdd


    ds.createOrReplaceTempView("user")

    //用户自定义普通函数
    spark.udf.register("addName", (x: String) => {
      "Name:" + x
    })
    spark.sql("select addName(name),age from user").show()

    //用户自定义聚合函数
    val udaf = new MyAgeAvgFunction
    spark.udf.register("avgAge", udaf)
    spark.sql("select avgAge(age) from user").show()


    //释放资源
    spark.stop()

  }


  //自定义聚合函数(弱类型)
  class MyAgeAvgFunction extends UserDefinedAggregateFunction {
    //函数输入的数据结构
    override def inputSchema: StructType = {
      new StructType().add("age", LongType)
    }

    //计算时缓冲区的数据结构
    override def bufferSchema: StructType = {
      new StructType().add("age", LongType).add("count", LongType)

    }

    //函数返回的数据类型
    override def dataType: DataType = DoubleType

    //函数是否稳定
    override def deterministic: Boolean = true

    //缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    //根据查询结果更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }

    //多个节点的缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      //sum
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      //count
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    //计算
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0).toDouble / buffer.getLong(1)
    }
  }


}

case class User(name: String, age: Long)

case class AvgBuffer(var sum: Long, var count: Long)

//自定义聚合函数(强类型)，需要使用dsl风格
class MyAgeAvgFunction2 extends Aggregator[User, AvgBuffer, Double] {
  //缓冲区初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  //每个数据计算
  override def reduce(b: AvgBuffer, a: User): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  //缓冲区合并
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  //完成计算结果
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  //自定义对象用product
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

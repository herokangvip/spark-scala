package com.hk.scala

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SqlTest {
  def main(args: Array[String]): Unit = {
    //
    val conf = new SparkConf().setMaster("local[1]").setAppName("sql")
    conf.set("spark.testing.memory", "536870912") //后面的值大于512m即可
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Word Count")
      .config(conf)
      .getOrCreate()

    val df: DataFrame = spark.read.json("in/user.json")
    //val df: DataFrame = spark.read.format("json").load("in/user.json")
    //写文件
    //df.write.format("json").mode("append").save("file:///xxx")


    //进行rdd和dataFrame和dataSet的转换需要引入隐式转换，
    //这里的spark是指sparkSession对象不是包名
    import spark.implicits._

    //structureStreaming内容
    //import org.apache.spark.sql.functions._
    //df.groupBy(window($"ts","10 minutes","4minutes"),$"word")

    //df.show()
    df.createOrReplaceTempView("user")

    spark.sql("select * from user").show()

    //释放资源
    spark.stop()

  }

  case class User(name: String, age: Long)

}

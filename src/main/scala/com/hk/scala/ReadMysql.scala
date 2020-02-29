package com.hk.scala

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从mysql读取数据
 */
object ReadMysql {
  def main(args: Array[String]): Unit = {
    //本地测试使用local模式，内存不够用需要设置testingMemory，线上不用
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("wordCount")
    conf.set("spark.testing.memory", "536870912") //后面的值大于512m即可
    val sc: SparkContext = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/mfcq"
    val userName = "root"
    val passWord = "root"

    //必需要有上下限,spark会根据设置的分区数对数据进行切分
    val sql = "select * from user where id>=? and id<=?"
    val jdbcRDD = new JdbcRDD(sc,
      () => {
        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, userName, passWord)
        connection
      },
      sql,
      1,
      99999999,
      1,
      (rs) => {
        println(rs.getString(1) + ":" + rs.getInt(2))
      }
    )

    jdbcRDD.collect()


    //保存数据到mysql
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("zhangsan", 20)))
    rdd.foreach({
      case (userName, age) => {
        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, userName, passWord)
        // TODO:这里创建连接有问题，每个数据都会创建,放到外面也不行，外面的代码在Driver执行不在Excutor
      }
    })
    // TODO: 解决方案 ，如果10w数据10个分区只创建10个连接，缺点和mapPartitions一样效率高了但是有可能oom
    rdd.foreachPartition(data => {
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, userName, passWord)
      data.foreach({
        case (userName, age) => {
          // TODO: 插入数据
        }
      })
    })
    sc.stop()
  }
}

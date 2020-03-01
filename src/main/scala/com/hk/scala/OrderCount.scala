package com.hk.scala

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计累计下单量
  *
  * @author k
  * @version 1.0
  */
object OrderCount {
  def main(args: Array[String]): Unit = {
    val ssc = buildStreamingContext()
    val kafkaParams = buildKafkaParams()
    val topicName = "test-topic"
    //从kafka接收数据
    val kafkaTopicDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Iterable(topicName), kafkaParams))

    val pairWord = kafkaTopicDS.map(_.value)
    //对每个Rdd中的每条数据进行处理
    pairWord.foreachRDD(rdd => {
      val repartitionRDD = rdd.repartition(3)
      //https://www.cnblogs.com/fillPv/p/5392186.html
      repartitionRDD.foreachPartition(loopAdd) //对重新分区后的rdd执行loopAdd累加函数
    })
    ssc.start() //启动
    ssc.awaitTermination() //等待终止命令
  }


  def buildStreamingContext(): StreamingContext = {
    //val conf = new SparkConf().setAppName("wordcount").setMaster("spark://192.168.137.201:7077")
    val conf = new SparkConf().setAppName("orderCount").setMaster("local[1]")
    conf.set("spark.executor.memory", "512M") //executor memory是每个节点上占用的内存。每一个节点可使用内存
    conf.set("spark.executor.cores", "2") //spark.executor.cores：顾名思义这个参数是用来指定executor的cpu内核个数，分配更多的内核意味着executor并发能力越强，能够同时执行更多的task
    conf.set("spark.cores.max", "2") //spark.cores.max：为一个application分配的最大cpu核心数，如果没有设置这个值默认为spark.deploy.defaultCores
    conf.set("spark.logConf", "True") //#当SparkContext启动时，将有效的SparkConf记录为INFO。
    conf.set("spark.testing.memory", "536870912")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Milliseconds(200))
    // offset保存路径
    val checkpointPath = "D:\\hadoop\\checkpoint\\kafka-direct"
    ssc.checkpoint(checkpointPath)
    return ssc
  }

  def buildKafkaParams(): Map[String, Object] = {
    val bootstrapServers = "127.0.0.1:9092"
    val groupId = "test-group"
    val maxPoll = 500
    val kafkaParams = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    kafkaParams
  }

  //把所有数据records定义为Iterator类型，方便我们遍历
  def loopAdd(records: Iterator[String]): Unit = {
    var orderNum = 0
    records.foreach(p => {
      orderNum = orderNum + p.trim.toInt
    })
    //数据写入到mysql
    writeToDb(orderNum)
  }

  def writeToDb(orderNum: Int): Unit = {
    //注意，conn和stmt定义为var不能是val
    var conn: Connection = null
    var stmt: PreparedStatement = null
    try {
      //连接数据库
      val url = "jdbc:mysql://localhost:3306/mfcq?useSSL=false&characterEncoding=utf8&serverTimezone=UTC" //地址+数据库
      val user = "root"
      val password = "root"
      conn = DriverManager.getConnection(url, user, password)
      val sql = "update test set num=num+? where id=3"
      stmt = conn.prepareStatement(sql)
      stmt.setInt(1, orderNum)
      stmt.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (stmt != null)
        stmt.close()
      if (conn != null)
        conn.close()
    }
  }


}

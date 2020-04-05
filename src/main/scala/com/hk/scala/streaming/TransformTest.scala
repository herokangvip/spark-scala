package com.hk.scala.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


/**
 * 本例：
 * 1：transform和map的区别
 * 2：foreachRDD
 */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("wordCount")
    conf.set("spark.testing.memory", "536870912") //后面的值大于512m即可

    //采集周期：以指定的时间周期采集数据,sparkStreaming的数据是按周期采集的
    val sc: StreamingContext = new StreamingContext(conf, Seconds(3))
    sc.sparkContext.setCheckpointDir("xxx")

    //从kafka读取数据
    val bootstrapServers = "127.0.0.1:9092"
    val groupId = "test-group"
    val topic = "test-topic"
    val maxPoll = 500
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    val kafkaInput: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(sc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Iterable(topic),
        kafkaParams))

    //窗口函数,窗口大小和滑动长度为采集周期的整数倍，因为窗口是整合多个采集周期的数据
    // spark只有滑动窗口，第二个参数步长不传的话默认值是一个采集周期，即滑动一个rdd，相比而言Flink的窗口机制更丰富完善
    //Flink支持滚动、滑动、计数、session窗口，而且对EventTime的支持更好，缺点Sql比较弱与Hive集成比较差，最新版本已经生产支持了Hive
    //有待验证
    val windowDstream = kafkaInput.window(Seconds(9), Seconds(3))

    //从消息中取出数据
    val stream: DStream[String] = windowDstream.flatMap(data => data.value().split(" "))
    //val stream: DStream[String] = kafkaInput.flatMap(data => data.value().split(" "))

    val mapStream: DStream[(String, Int)] = stream.map((_, 1))

    //foreachRDD:不考虑窗口情况下只有一个rdd，有窗口时一个窗口内包含几个采集周期这里就有几个rdd
    mapStream.foreachRDD(rdd=>{
      rdd.map(a=>"a")
    })


    //transform和map的区别
    // TODO: 这个地方的代码只执行一遍在Driver执行
    /*mapStream.transform {
      case rdd => {
        // TODO: 这个地方的代码会每个采集周期执行，在Driver，比如黑名单需求需要每次滑动重新加载黑名单
        // TODO: 而不是启动的时候加载一次
        rdd.map {
          case x => {
            // TODO: 这个地方的代码执行n遍在Excutor执行
            x
          }
        }
      }
    }*/

    //启动采集器，流式处理要加最后这两步
    sc.start()
    //Driver等待采集器的执行
    sc.awaitTermination()

  }
}

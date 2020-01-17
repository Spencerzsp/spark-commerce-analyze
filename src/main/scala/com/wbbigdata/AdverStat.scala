package com.wbbigdata

import java.lang

import conf.ConfigurationManager
import constant.Constants
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.AdBlackListDAO

object AdverStat {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")
    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    val sc = sparkSession.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    //获取kafka集群信息
    val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    val kafkaParams = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    //adRealTimeDStream: 里面是一个一个的Message[key,value],value才是真正的log日志，key决定分发到哪儿
    //adRealTimeDStream：DStream[RDD RDD RDD ...] , RDD[message], message:key value
    val adRealTimeDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParams)
    )

    //取出DStream里面每一条数据的value
    //adRealTimeValueDStream[RDD RDD RDD ...]
    //RDD[String]
    // String:timestap province city userid adid
    val adRealTimeValueDStream = adRealTimeDStream.map(item => item.value())

    adRealTimeValueDStream.foreachRDD(rdd => rdd.foreach(println))

    //transform算子关联DStram和RDD
    val adRealTimeFilterDStream = adRealTimeValueDStream.transform{
      logRDD =>

        val blackListArray = AdBlackListDAO.findAll()

        val userIdArray = blackListArray.map(item => item.userid)

        logRDD.filter{
          case log =>
            val logSplit = log.split(" ")
            val userId = logSplit(3).toLong
            !userIdArray.contains(userId)
        }

    }

    adRealTimeFilterDStream.foreachRDD(rdd => rdd.foreach(println))


    ssc.start()
    ssc.awaitTermination()

  }
}

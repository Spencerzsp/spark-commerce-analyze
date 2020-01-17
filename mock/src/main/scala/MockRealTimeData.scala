import java.util.Properties

import conf.ConfigurationManager
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object MockRealTimeData {

  def createKafkaProducer(broker: String) = {

    //创建配置对象
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    //根据配置创建kafka生产者
    new KafkaProducer[String, String](prop)
  }

  def generateMockData(): Array[String] = {

    val array = ArrayBuffer[String]()
    val random = new Random()

    //模拟实时数据
    for (i <- 0 to 50) {
      val timestamp = System.currentTimeMillis()
      val province = random.nextInt(10)
      val city = province
      val adid = random.nextInt(20)
      val userid = random.nextInt(100)

      //拼接实时数据
      array += timestamp + " " + province + " " + city + " "+ userid + " " + adid
    }

    array.toArray
  }

  def main(args: Array[String]): Unit = {

    //获取配置文件中kafka的配置参数
    val broker = ConfigurationManager.config.getString("kafka.broker.list")
    val topic = ConfigurationManager.config.getString("kafka.topics")

    //创建kafka消费者
    val kafkaProducer = createKafkaProducer(broker)

    while (true){

      //随机产生实时数据并通过kafka生产者发送到kafka集群中,每次发送50条
      for (item <- generateMockData()) {
        kafkaProducer.send(new ProducerRecord[String, String](topic, item))
      }

      println("-------------------------------")

      Thread.sleep(5000)
    }
  }

}

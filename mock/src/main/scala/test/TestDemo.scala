package test

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import utils.DataUtis.DATE_FORMAT

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

case class Person(name: String, age: String, addr: String)
object TestDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("TestDemo")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

//    val random = new Random()
//    val str = DateTime.now() + random.nextInt(20).toString
//    println(str)

//    for (i <- 0 to 5)
//      println(i)
//    val array = Array((1, "张三,20"), (2, "李四,30"), (1, "王五,40"), (2, "赵六,50"), (1, "田七,60"))
//    val dataRDD = sc.makeRDD(array)
//
//    //RDD[Int, Iterable]
//    val id2GroupRDD = dataRDD.groupByKey().repartition(1)
//    val fullInfo2RDD = id2GroupRDD.map {
//      case (id, studentInfo) =>
//        for (stu <- studentInfo) {
//          val name = stu.split(",")(0)
//          val age = stu.split(",")(1)
//
//          println(name, age)
//        }
//    }
//    fullInfo2RDD.foreach(println)

    val personDF = spark.sql("select * from person")
    import spark.implicits._
    val personRDD = personDF.as[Person].rdd
//    personRDD.foreach(println)

    val map2PersonRDD = personRDD.map(
      person => (person.addr, person)
    )
//    map2PersonRDD.foreach(println)

    val person2GroupRDD = map2PersonRDD.groupByKey()

    val fullPersonInfoRDD = person2GroupRDD.map {
      case (addr, iter) =>
        var name = ""
        var age = ""
        for (person <- iter) yield {
          name = person.name
          age = person.age
          val personInfo = "addr: " + addr + " name: " + name + " age: " + age
          personInfo
        }

    }
    fullPersonInfoRDD.foreach(println)

//    val rows = ArrayBuffer[Person]
  }

}

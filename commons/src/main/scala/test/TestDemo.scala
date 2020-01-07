package test

import java.util
import java.util.Date

import org.joda.time.DateTime
import utils.{DateUtils, StringUtils}

import scala.util.Random

object TestDemo {

  def main(args: Array[String]): Unit = {

//    val random = new Random()
//    val i = random.nextInt(10)
//    println(String.valueOf(i))

//    val date = DateTime.now().toString("yyyy-MM-dd HH:mm:ss")
//    val dateTime = DateTime.now()
//    println(date + "\t" + dateTime)

//    val date2 = DateUtils.stampToDate("1578045452000")
//    println(date2)

//    val stamp = DateUtils.dateToStamp("2020-01-03 17:57:32")
//    println(stamp)
//
//    val str = StringUtils.trimComma("hello,spark,scala,")
//
//    val buffer = new StringBuffer("hello,spark,scala,")
//    println(str)
//    println(buffer.length())
//    println(buffer.toString)

//    val dateTime = DateTime.now().toString("yyyy-MM-dd HH:mm:ss")
//    println(DateTime.now())
//    println(dateTime)
//
//    val dateTimeStamp = DateUtils.dateToStamp(dateTime)
//    println(dateTimeStamp)
//
//    val parseTime = DateUtils.parseTime("2020-01-03 14:20:39")
//    println(parseTime)
////    DateUtils.formatTime()
//
//    val formatTime = DateUtils.formatTime(new Date("Fri Jan 03 14:20:39 CST 2020"))
//    println(formatTime)

    val line = "startAge=20|endAge=50|professionals=|cities=|sex=|keywords=|categoryIds="
    val splits = line.split("\\|")
    val values = for (split <- splits) yield {
      val index = split.indexOf("=")
      if (split.substring(0, index) == "endAge") {
        val str = split.substring(index + 1)
        str
      }
    }

    val list = new util.ArrayList[Any]()
    for (v <- values) {
//      println(v)
      if (v != ()) {
        list.add(v)
      }
    }
    println(list.get(0))


//    println(splits(0).indexOf("="))
//    val value = for (split <- splits) yield {
////      println(split)
//      val index = split.indexOf("=")
//      if (split.substring(0, index) == "endAge") {
//        split.substring(index + 1)
//      }
//    }
//    println(value(0))

  }

}

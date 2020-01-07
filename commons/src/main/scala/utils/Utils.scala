package utils

import java.text.SimpleDateFormat

import org.joda.time.DateTime

import scala.util.Random

object Utils {

}
object DataUtis {

  val DATE_FORMAT = "yyyy-MM-dd"

  /**
    * 获取当天日期(yyyy-MM-dd)
    */
  def getTodayDate() = {
    DateTime.now().toString(DATE_FORMAT)
  }

  /**
    * 获取昨天的日期
    */
  def getYesterdayDate() = {
    DateTime.now().minusDays(1).toString(DATE_FORMAT)
  }

  def getDate(): Unit ={
    val random = new Random()
    DateTime.now().toString(DATE_FORMAT) + random.nextInt(20)
  }

}

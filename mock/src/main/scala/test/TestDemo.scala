package test

import org.joda.time.DateTime
import utils.DataUtis.DATE_FORMAT

import scala.util.Random

object TestDemo {

  def main(args: Array[String]): Unit = {

    val random = new Random()
    val str = DateTime.now() + random.nextInt(20).toString
    println(str)

  }

}
